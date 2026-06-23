---
name: pols-dbdriver
description: Guía de uso y desarrollo de pols-dbdriver, una capa de abstracción para bases de datos SQL Server (sqlsrv/sqlsrv2008).
---

# `pols-dbdriver`

`pols-dbdriver` es una librería diseñada para simplificar la interacción con diferentes motores de bases de datos a través de una interfaz unificada. Proporciona atajos útiles para construir consultas SQL parametrizadas, paginación automática, inserción/actualización inteligente (`save`) y generación dinámica de esquemas de tablas.

## Archivos Principales del Proyecto

- **Lógica del Driver**: [src/index.ts](file:///d:/Coding/pols-dbdriver/src/index.ts) contiene la implementación de la clase principal y todas las definiciones de tipos.
- **Ejemplo y Pruebas**: [test/index.ts](file:///d:/Coding/pols-dbdriver/test/index.ts) ilustra cómo conectar y consultar una base de datos usando SQL Server.

---

## 1. Conexión e Inicialización

Para crear una instancia de la base de datos, se utiliza la clase [`PDBDriver`](file:///d:/Coding/pols-dbdriver/src/index.ts#L171), pasando un objeto de configuración del tipo [`PDBDriverParams`](file:///d:/Coding/pols-dbdriver/src/index.ts#L44).

### Motores Soportados (`PDriverNames`)
Los motores soportados y definidos en [`PDriverNames`](file:///d:/Coding/pols-dbdriver/src/index.ts#L32) son:
- `'sqlsrv2008'` (SQL Server 2008)
- `'sqlsrv'` (SQL Server moderno)

### Ejemplo de Configuración e Inicialización

```typescript
import { PDBDriver, PDriverNames } from './src/index';

// Configuración para SQL Server
const db = new PDBDriver({
    driver: PDriverNames.sqlsrv,
    host: 'localhost',
    database: 'MiBaseDeDatos',
    user: 'sa',
    password: 'PasswordSegura',
    rowsPerPage: 20
});

async function main() {
    // Conectar al servidor
    await db.connect();
    
    // ... Operaciones ...
    
    // Cerrar la conexión
    await db.close();
}
```

---

## 2. Ejecución de Consultas (Queries)

### Parámetros en Consultas
Las consultas admiten binding automático de parámetros usando el prefijo `$`. La librería se encarga de escapar y sanitizar los valores automáticamente.

- **`query(command, parameters, groupColumns)`**: Ejecuta una consulta y devuelve un objeto [`PQueryResults`](file:///d:/Coding/pols-dbdriver/src/index.ts#L80) que contiene las filas, cantidad de registros, la sentencia final y la estructura de columnas.
- **`queryOne(command, parameters, groupColumns)`**: Devuelve la primera fila del resultado o `null` si está vacío.
- **`exec(command)`**: Ejecuta un comando SQL que no retorna filas (por ejemplo, `CREATE INDEX`, `DROP TABLE`).

```typescript
// Consulta parametrizada (Híbrida)
const resultados = await db.query(
    'SELECT * FROM Usuarios WHERE Estado = @estado AND Rol IN (@roles) AND FechaRegistro >= @fecha',
    {
        estado: 'Activo',
        roles: ['Admin', 'Editor'], // Se interpola de forma segura automáticamente
        fecha: new Date('2026-01-01') // Se parametriza nativamente con SQL Server (@fecha)
    }
);

console.log(resultados.rows); // Array de filas
```

### Parametrización Nativa Híbrida
Cuando ejecutas una consulta parametrizada pasando el objeto de `parameters`, la librería maneja la sustitución de forma inteligente (híbrida):
1. **Tipos Primitivos** (`string`, `number`, `boolean`, `Date`, `null`): Al usar `@parameterName` en la sentencia SQL, el driver de SQL Server los asocia automáticamente y los envía parametrizados directamente al motor. Esto protege contra SQL Injection y permite la reutilización de planes de ejecución.
2. **Arrays y Expresiones SQL**: Debido a limitaciones de SQL Server, los arrays (ej. `@roles`) y objetos de expresión se escapan y se interpolan de forma directa en el texto del comando de manera segura.

### Escapado Manual y Template Literals (`escape`)
Además del binding automático con `@`, puedes pasar datos de forma directa a la consulta utilizando el método `escape()` dentro de *template literals* de JavaScript. Esto te permite construir la consulta e inspeccionar o copiar la sentencia SQL completa en caso de error.

El método `escape()` soporta números, booleanos (`1` o `0`), strings (agregando comillas simples y escapando comillas internas), fechas y **arrays** (formateándolos como listas separadas por comas, ideal para cláusulas `IN`).

```typescript
const estado = 'Activo';
const roles = ['Administrador', 'Editor'];

// Uso directo con template literals
const queryStr = `
    SELECT * 
    FROM Usuarios 
    WHERE Estado = ${db.escape(estado)} 
      AND Rol IN (${db.escape(roles)})
`;

const resultados = await db.query(queryStr);
```

---

## 3. Atajos y Métodos Auxiliares de Consulta

### Selección Avanzada (`select`)
El método [`select`](file:///d:/Coding/pols-dbdriver/src/index.ts#L658) permite realizar consultas SELECT estructuradas usando parámetros del tipo [`PSelectParams`](file:///d:/Coding/pols-dbdriver/src/index.ts#L60).

Soporta:
- **Paginación automática**: Al pasar la propiedad `page`, calcula automáticamente el offset y count basándose en `rowsPerPage`.
- **Filtros rápidos (`filter`)**: Genera búsquedas `LIKE` inteligentes y seguras en múltiples campos utilizando palabras clave separadas por espacios. Admite negaciones con prefijo `-` y comodines `*`.
- **Agrupamiento de columnas**: Si `groupColumns` es `true`, agrupa los nombres de columnas con puntos (ej. `tabla.columna`) en objetos anidados.

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
    page: 1 // Devuelve la página 1 según la configuración de rowsPerPage
});
```

### Contar Registros (`count`)
El método [`count`](file:///d:/Coding/pols-dbdriver/src/index.ts#L654) permite contar de manera rápida la cantidad de registros que cumplen con una estructura de [`PSelectParams`](file:///d:/Coding/pols-dbdriver/src/index.ts#L60).

```typescript
const total = await db.count({
    from: 'Productos',
    where: 'Stock < 10'
});
```

---

## 4. Guardado Automático e Inserciones (`save` y `batchSave`)

La librería expone herramientas para evitar escribir sentencias manuales de `INSERT` y `UPDATE`.

### Guardar/Actualizar (`save`)
Genera automáticamente un `INSERT` si no se provee la condición `where`, o un `UPDATE` si se especifica una condición `where`.

```typescript
// INSERT automático
const insertResult = await db.save('Usuarios', {
    values: {
        Nombre: 'Juan',
        Email: 'juan@example.com',
        Activo: true,
        FechaCreacion: new Date()
    }
});
console.log('ID insertado:', insertResult.lastID);

// UPDATE automático
await db.save('Usuarios', {
    values: {
        Activo: false
    },
    where: 'Id = 5'
});
```

### Guardado Masivo (`batchSave`)
Permite guardar múltiples registros en distintas tablas de forma eficiente en una sola ejecución.

```typescript
await db.batchSave(
    {
        table: 'LogActividades',
        values: { Actividad: 'Ingreso al sistema', UsuarioId: 5 }
    },
    {
        table: 'Usuarios',
        values: { UltimoAcceso: new Date() },
        where: 'Id = 5'
    }
);
```

---

## 5. Manejo de Transacciones

Permite realizar un grupo de operaciones asegurando consistencia.

```typescript
try {
    await db.beginTransaction();
    
    await db.save('Cuentas', {
        values: { Saldo: { expression: 'Saldo - 100' } },
        where: 'Id = 1'
    });
    
    await db.save('Cuentas', {
        values: { Saldo: { expression: 'Saldo + 100' } },
        where: 'Id = 2'
    });
    
    await db.commitTransaction();
} catch (error) {
    await db.rollbackTransaction();
    throw error;
}
```

---

## 6. Sincronización y Modificación del Esquema (DDL)

[`pols-dbdriver`](file:///d:/Coding/pols-dbdriver/src/index.ts) incluye helpers potentes para migrar y actualizar esquemas de base de datos sin escribir sentencias DDL complejas para cada motor.

- **`buildTable`**: Crea una tabla o añade/actualiza columnas si la tabla ya existe, aplicando tipos homogenizados, valores por defecto, comentarios y llaves primarias.
- **`buildForeignKeys`**: Crea o elimina restricciones de llave foránea de manera declarativa.

```typescript
// Crear o actualizar la estructura de una tabla
await db.buildTable({
    schema: 'dbo',
    table: 'Clientes',
    comments: 'Tabla que almacena la información básica de clientes',
    fields: {
        Id: { type: PFieldTypes.int, primaryKey: true, autoincrement: true },
        Nombre: { type: PFieldTypes.varchar, length: 150, notNull: true },
        CodigoIdentificacion: { type: PFieldTypes.varchar, length: 20, notNull: false },
        FechaRegistro: { type: PFieldTypes.datetime, default: new Date() }
    }
});

// Sincronizar llaves foráneas
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
