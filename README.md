# pols-dbdriver

`pols-dbdriver` es una librería de abstracción y utilidades ligera diseñada para simplificar y agilizar la interacción con bases de datos **Microsoft SQL Server** (con soporte completo para versiones modernas de SQL Server y compatibilidad hacia atrás con SQL Server 2008). 

Proporciona una interfaz unificada para realizar consultas parametrizadas nativas, paginación optimizada en un solo viaje de red, operaciones declarativas de guardado (`INSERT` / `UPDATE`), control de transacciones y migración dinámica de esquemas de bases de datos.

---

## Características Principales

*   🔌 **Conexión Simplificada**: Configuración rápida mediante URI o parámetros estructurados.
*   🔒 **Parametrización Híbrida Inteligente**: Soporte nativo de parámetros SQL Server con prefijo `@` para tipos primitivos (previniendo inyección SQL y optimizando la caché de planes) y escapado automático seguro para colecciones (`Array`) y expresiones SQL complejas.
*   ⚡ **Paginación en un Solo Viaje**: El método `select()` utiliza la cláusula de ventana `COUNT(*) OVER()` para obtener la paginación y el conteo total en una sola consulta.
*   💾 **Persistencia Declarativa**: Métodos `save()` y `batchSave()` que deciden de forma inteligente si realizar un `INSERT` o `UPDATE` según la presencia de condiciones.
*   🛠️ **Migración Dinámica de Esquemas**: Crea y sincroniza tablas (`buildTable`), llaves foráneas (`buildForeignKeys`) y rutinas (`buildProcedureOrFunction`) adaptándose a las limitaciones de cada versión de SQL Server.

---

## Instalación

Instala el driver y sus dependencias requeridas usando npm:

```sh
npm install pols-dbdriver mssql
```

*Nota: Asegúrate de tener instalado `pols-utils` en tu proyecto, ya que es una dependencia requerida para el formateo de datos y estructuras.*

---

## Guía de Uso

### 1. Inicialización y Conexión

Puedes configurar e inicializar el driver utilizando la clase `PDBDriver` y conectarte a tu instancia de base de datos:

```typescript
import { PDBDriver, PDriverNames } from 'pols-dbdriver';

const db = new PDBDriver({
    driver: PDriverNames.sqlsrv, // O PDriverNames.sqlsrv2008
    host: '127.0.0.1',
    database: 'MiBaseDeDatos',
    user: 'sa',
    password: 'PasswordSeguro',
    rowsPerPage: 20 // Cantidad de filas por página por defecto
});

async function main() {
    await db.connect();
    console.log('Conexión exitosa:', db.connected);

    // ... realizar operaciones ...

    await db.close();
}
```

### 2. Consultas Parametrizadas (Híbridas)

La librería utiliza un enlace de parámetros híbrido usando el prefijo nativo `@`. Los tipos primitivos se envían parametrizados al motor de base de datos, mientras que los arrays y expresiones SQL se escapan y se interpolan de forma segura en la cadena SQL automáticamente.

```typescript
const resultados = await db.query(
    'SELECT * FROM Clientes WHERE Estado = @estado AND Rol IN (@roles) AND FechaRegistro >= @fecha',
    {
        estado: 'Activo',
        roles: ['Admin', 'Editor'], // Se escapa e interpola: 'Admin', 'Editor'
        fecha: new Date('2026-01-01') // Se parametriza de forma nativa (@fecha)
    }
);

console.log(resultados.rows); // Array de registros
```

### 3. Escapado Manual y Template Literals

Si prefieres tener visibilidad completa de la sentencia SQL compilada (por ejemplo, para depuración), puedes utilizar el método `escape()` dentro de *template literals*:

```typescript
const nombre = "O'Connor"; // Las comillas simples se escapan a '' automáticamente
const roles = ['Usuario', 'Moderador'];

const queryStr = `
    SELECT * 
    FROM Clientes 
    WHERE Nombre = ${db.escape(nombre)} 
      AND Rol IN (${db.escape(roles)})
`;

const resultados = await db.query(queryStr);
```

### 4. Paginación y Filtrado Rápido (`select`)

El método `select()` encapsula la lógica de paginación y búsqueda textual rápida en múltiples columnas. Trae la página actual y calcula el conteo total en **un solo viaje de red** usando `COUNT(*) OVER()`.

```typescript
const resultado = await db.select({
    from: 'Clientes C',
    select: 'C.Id, C.Nombre, P.Nombre as Pais',
    joins: 'LEFT JOIN Paises P ON C.PaisId = P.Id',
    where: ['C.Activo = 1'],
    filter: {
        text: 'Juan -Perez', // Busca "Juan" y excluye "Perez"
        fields: ['C.Nombre', 'C.Apellido']
    },
    order: 'C.Nombre ASC',
    page: 1 // Trae la página 1 usando la configuración de rowsPerPage del driver
});

console.log(resultado.rows);       // Los 10 registros de la página
console.log(resultado.rowsCount);  // Total de filas que coinciden en toda la BD
```

### 5. Inserción y Actualización Declarativa (`save`)

Evita escribir sentencias manuales de `INSERT` o `UPDATE`. El método `save()` determina la operación correcta basándose en si pasas una condición `where`.

```typescript
// INSERT automático (al no enviar condición "where")
const insertResult = await db.save('Clientes', {
    values: {
        Nombre: 'Carlos',
        Email: 'carlos@example.com',
        FechaRegistro: new Date()
    }
});
console.log('ID autogenerado:', insertResult.lastID);

// UPDATE automático (al enviar la condición "where")
await db.save('Clientes', {
    values: {
        Email: 'carlos.nuevo@example.com'
    },
    where: 'Id = 5' // O un array de condiciones ['Id = 5', 'Activo = 1']
});
```

### 6. Control de Transacciones

Maneja transacciones garantizando atomicidad y consistencia en tus operaciones:

```typescript
try {
    await db.beginTransaction();

    await db.save('Cuentas', {
        values: { Saldo: { expression: 'Saldo - 50' } },
        where: 'Id = 1'
    });

    await db.save('Cuentas', {
        values: { Saldo: { expression: 'Saldo + 50' } },
        where: 'Id = 2'
    });

    await db.commitTransaction();
} catch (error) {
    await db.rollbackTransaction();
    throw error;
}
```

### 7. Administración Dinámica del Esquema (DDL)

Sincroniza y actualiza la estructura de tus tablas y llaves foráneas dinámicamente:

```typescript
import { PFieldTypes } from 'pols-dbdriver';

// Crear o actualizar la estructura de una tabla
await db.buildTable({
    schema: 'dbo',
    table: 'Clientes',
    comments: 'Almacena la información de clientes de la empresa',
    fields: {
        Id: { type: PFieldTypes.int, primaryKey: true, autoincrement: true },
        Nombre: { type: PFieldTypes.varchar, length: 150, notNull: true },
        Activo: { type: PFieldTypes.boolean, default: true }
    }
});

// Sincronizar llaves foráneas de forma declarativa
await db.buildForeignKeys({
    schema: 'dbo',
    table: 'Clientes',
    fields: {
        PaisId: {
            foreignKey: {
                schema: 'dbo',
                table: 'Paises',
                field: 'Id'
            }
        }
    }
});
```

### 8. Gestión de Stored Procedures y Functions

Crea, actualiza u obtén las definiciones de procedimientos y funciones en tu base de datos de manera idempotente y compatible con SQL Server 2008+:

```typescript
import { PDBRoutineTypes } from 'pols-dbdriver';

// Crear o actualizar un procedimiento almacenado
await db.buildProcedureOrFunction({
    schema: 'dbo',
    name: 'ObtenerClientesActivos',
    type: PDBRoutineTypes.procedure,
    definition: `
        CREATE PROCEDURE dbo.ObtenerClientesActivos
            @fechaMinima DATETIME
        AS
        BEGIN
            SET NOCOUNT ON;
            SELECT * FROM Clientes WHERE Activo = 1 AND FechaRegistro >= @fechaMinima
        END
    `
});

// Recuperar el tipo y definición SQL de un procedimiento o función
const metadata = await db.getProcedureOrFunction('ObtenerClientesActivos', 'dbo');
if (metadata) {
    console.log(metadata.type);       // PDBRoutineTypes.procedure
    console.log(metadata.definition); // Código SQL completo
}
```

---

## Licencia

Este proyecto está bajo la licencia [ISC](LICENSE).