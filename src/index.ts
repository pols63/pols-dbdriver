import { PDate, PUtils } from 'pols-utils'
import { URecord } from 'pols-utils/dist/types'

const mssql = (() => {
	try {
		return require('mssql')
	} catch {
		return null
	}
})()
const mariadb = (() => {
	try {
		return require('mariadb')
	} catch {
		return null
	}
})()
const sqlite3 = (() => {
	try {
		return require('sqlite3')
	} catch {
		return null
	}
})()
const pg = (() => {
	try {
		return require('pg')
	} catch {
		return null
	}
})()

export enum PDriverNames {
	sqlsrv2008 = 'sqlsrv2008',
	sqlsrv = 'sqlsrv',
	mariadb = 'mariadb',
	postgresql = 'postgresql',
	sqlite = 'sqlite'
}

export type PTableRow = {
	[fieldName: string]: string | number | boolean | Date | null
}

export type PDBDriverParams = {
	rowsPerPage: number
} & ({
	driver: PDriverNames.mariadb | PDriverNames.sqlsrv | PDriverNames.sqlsrv2008 | PDriverNames.postgresql
	uri?: string
	host?: string
	port?: number
	user?: string
	password?: string
	database?: string
	requestTimeout?: number
} | {
	driver: PDriverNames.sqlite
	file: string
})

export type PSelectParams = {
	from: string
	select?: string,
	filter?: {
		text: string
		fields: string[]
	}
	where?: string | string[]
	having?: string | string[]
	joins?: string
	page?: number
	group?: string
	order?: string
	limit?: {
		offset: number
		count: number
	}
}

export type PQueryResults<T = PTableRow> = {
	readonly rows: T[]
	readonly rowsCount: number
	readonly statement: string
	readonly lastID?: number
}

export type PSaveParams = {
	values: {
		[fieldName: string]: string | number | Date | boolean | { expression: string }
	}
	where?: string | string[]
	returning?: string
}

export type PBatchSaveParams = PSaveParams & { table: string }

export enum PFieldTypes {
	boolean = 'boolean',
	varchar = 'varchar',
	date = 'date',
	datetime = 'datetime',
	smallint = 'smallint',
	int = 'int',
	bigint = 'bigint',
	float = 'float',
	double = 'double',
	decimal = 'decimal',
	numeric = 'numeric',
	text = 'text',
}

export type PFieldDefinition = {
	comments?: string
	notNull?: boolean
	primaryKey?: boolean
} & ({
	type: PFieldTypes.varchar
	length: number
	default?: string
} | {
	type: PFieldTypes.bigint | PFieldTypes.int | PFieldTypes.float | PFieldTypes.double | PFieldTypes.smallint
	default?: number
	autoincrement?: boolean
} | {
	type: PFieldTypes.numeric | PFieldTypes.decimal
	length: [number, number]
	default?: number
	autoincrement?: boolean
} | {
	type: PFieldTypes.text
	default?: string
} | {
	type: PFieldTypes.boolean
	default?: boolean | number
} | {
	type: PFieldTypes.date | PFieldTypes.datetime
	default?: string | Date
})

export type PForeignKeyDefinition = {
	foreignKey: false | {
		schema?: string
		table: string
		field: string
	}
}

export type PBuildTableParams = {
	schema?: string
	table: string
	comments?: string
	fields: {
		[fieldName: string]: PFieldDefinition
	}
}

export type PBuildForeignKeysParams = {
	schema?: string
	table: string
	comments?: string
	fields: {
		[fieldName: string]: PForeignKeyDefinition
	}
}

export class PDBDriver {
	private engine: any

	private transactionInstance: any
	private _inTransaction = false
	private _connected = false

	readonly config: Readonly<PDBDriverParams>
	readonly fieldNameDelimiter: string

	get inTransaction() {
		return this._inTransaction
	}

	get connected() {
		return this._connected
	}

	get driver() {
		return this.config.driver
	}

	get resultsPerPage() {
		return this.config.rowsPerPage
	}

	constructor(config: PDBDriverParams) {
		if (config.rowsPerPage < 0) throw new Error(`La propiedad 'resultsPerPage' debe ser mayor a cero`)
		if (Math.ceil(config.rowsPerPage) != config.rowsPerPage) throw new Error(`La propiedad 'resultsPerPage' debe ser un número entero`)

		if (config.driver == 'mariadb') {
			this.fieldNameDelimiter = '`'
		} else {
			this.fieldNameDelimiter = '"'
		}

		this.config = config
	}

	/* Conexión con el servidor */
	async connect() {
		try {
			switch (this.config.driver) {
				case PDriverNames.sqlite:
					this.engine = new (sqlite3.verbose()).Database(this.config.file)
					break
				case PDriverNames.postgresql: {
					const client = new pg.Client({
						user: this.config.user,
						host: this.config.host,
						database: this.config.database,
						password: this.config.password,
						port: this.config.port,
						connectionString: this.config.uri,
						ssl: {
							rejectUnauthorized: false
						}
					})
					await (() => new Promise((resolve, reject) => {
						client.connect((err: any) => {
							if (err) {
								reject(err)
								return
							}
							this.engine = client
							resolve(null)
						})
					}))()
				} break
				case PDriverNames.mariadb: {
					this.engine = await mariadb.createConnection({
						database: this.config.database,
						host: this.config.host,
						user: this.config.user,
						password: this.config.password ?? '',
						port: this.config.port
					})
					break
				}
				case PDriverNames.sqlsrv2008:
				case PDriverNames.sqlsrv: {
					const pool = new mssql.ConnectionPool('uri' in this.config ? (this.config.uri) : {
						server: this.config.host,
						database: this.config.database,
						port: 1433,
						authentication: {
							type: 'default',
							// type: 'azure-active-directory-default',
							options: {
								userName: this.config.user,
								password: this.config.password,
							},
						},
						options: {
							// useUTC: false,
							encrypt: true,
							enableArithAbort: true,
							trustServerCertificate: true
						},
						requestTimeout: this.config.requestTimeout ?? 15000,
					})
					const connection = await pool.connect()
					/* Artificio para cambiar el UTC en el driver */

					const c = connection as any
					c.config.options.useUTC = false
					this.engine = connection
					break
				}
			}
			this._connected = true
		} catch (err) {
			throw new Error(`Ocurrió un error al intentar conectarse al servidor de base de datos: ${err.stack}`)
		}
	}

	async close() {
		if (this._inTransaction) await this.rollbackTransaction()
		switch (this.config.driver) {
			case PDriverNames.mariadb: {
				this.engine.destroy()
				break
			}
			case PDriverNames.sqlsrv2008:
			case PDriverNames.sqlsrv: {
				await this.engine.close()
				break
			}
			case PDriverNames.postgresql:
				await this.engine.end()
				this.engine.release()
				break
			case PDriverNames.sqlite: {
				const connection = this.engine
				connection.close()
				break
			}
		}
		this._connected = false
	}

	escape(value?: number | string | boolean | Date | PDate | { expression: string } | null | unknown, fullDate = true): string {
		const typeOfValue = typeof value
		if (value == null) {
			return 'null'
		} else if (typeOfValue == 'boolean') {
			if ([PDriverNames.sqlsrv, PDriverNames.sqlsrv2008].includes(this.config.driver)) {
				return value ? '1' : '0'
			} else {
				return value.toString()
			}
		} else if (typeOfValue == 'string') {
			return `'${(value as string).replace(/'/g, "''")}'`
		} else if (typeOfValue == 'number') {
			return `${value}`
		} else if (value instanceof Date) {
			if ([PDriverNames.sqlsrv, PDriverNames.sqlsrv2008].includes(this.config.driver)) {
				return `'${PUtils.Date.format(value, `@y@mm@dd${fullDate ? ' @hh:@ii:@ss' : ''}`)}'`
			} else {
				return `'${PUtils.Date.format(value, `@y-@mm-@dd${fullDate ? ' @hh:@ii:@ss' : ''}`)}'`
			}
		} else if (value instanceof PDate) {
			if ([PDriverNames.sqlsrv, PDriverNames.sqlsrv2008].includes(this.config.driver)) {
				return `'${value.toString(`@y@mm@dd${fullDate ? ' @hh:@ii:@ss' : ''}`)}'`
			} else {
				return `'${value.toString(`@y-@mm-@dd${fullDate ? ' @hh:@ii:@ss' : ''}`)}'`
			}
		} else if (typeOfValue == 'object' && 'expression' in (value as object)) {
			return (value as { expression: string }).expression
		} else {
			throw new Error(`Valor no válido para escapar: ${value}`)
		}
	}

	// private async run(command: string, resultsAreExpected = true, ignoreDateFormat = false) {
	private async run<T = PTableRow>(command: string, resultsAreExpected = true) {
		if (!this._connected) throw new Error(`El driver no está conectado con el servidor`)
		let results: T[] = null
		try {
			switch (this.config.driver) {
				case PDriverNames.postgresql:
					// this._connection.query(command, (err: any, res: any) => {
					// 	if (err) {
					// 		reject(err)
					// 	} else {
					// 		resolve(res.length ? res.map((r: any) => r.rows).flat() : res.rows)
					// 	}
					// })
					break
				case PDriverNames.sqlsrv:
				case PDriverNames.sqlsrv2008: {
					let request: any
					if (this._inTransaction) {
						request = this.transactionInstance.request()
					} else {
						request = this.engine.request()
					}
					results = (await request.query(/*sql*/`${command}`)).recordset
					break
				}
				case PDriverNames.mariadb:
					results = await this.engine.query(command)
					break
				case PDriverNames.sqlite: {
					const toExecute = resultsAreExpected ? this.engine.all : this.engine.exec
					results = await (() => new Promise((resolve, reject) => {
						toExecute.bind(this.engine)(command, function (error: Error, rows?: T[]) {
							if (error) {
								reject(error)
								return
							}
							resolve(rows)
						})
					}))()
					break
				}
			}
		} catch (err) {
			throw new Error(`Ocurrió un error al intentar ejecutar la siguiente sentencia:\n${err.message}\n${command}`)
		}
		return results
	}

	// async exec(command: string, ignoreDateFormat = false) {
	async exec(command: string) {
		// await this.run(command, false, ignoreDateFormat)
		await this.run(command, false)
	}

	async query<T = PTableRow>(command: string, groupColumns?: boolean): Promise<PQueryResults<T>> {
		const results = await this.run<T>(command, true)

		/* Agrupación de columnas */
		if (results?.length && groupColumns) {
			if (groupColumns) results.forEach(row => {
				const keys = Object.keys(row)
				for (const key of keys) {
					if (key.match(/\./)) {
						PUtils.Object.setValue(row as URecord, key, row[key])
						delete row[key]
					}
				}
			})
		}

		return {
			rows: results ?? [],
			rowsCount: results?.length ?? 0,
			statement: command
		}
	}

	async queryOne<T = PTableRow>(command: string, groupColumns?: boolean): Promise<T | null> {
		return (await this.query<T>(command, groupColumns)).rows[0] ?? null
	}

	beginTransaction(): Promise<void> {
		if (this._inTransaction) throw new Error(`El conector ya ha inicializado una transacción`)
		switch (this.driver) {
			case PDriverNames.sqlsrv2008:
			case PDriverNames.sqlsrv: {
				return new Promise((resolve, reject) => {
					this.transactionInstance = this.engine.transaction()
					this.transactionInstance.begin(err => {
						if (err) {
							reject(err)
						} else {
							this._inTransaction = true
							resolve()
						}
					})
				})
			}
		}
	}

	commitTransaction(): Promise<void> {
		if (!this._inTransaction) throw new Error(`El conector no ha inicializado ninguna transacción`)
		switch (this.driver) {
			case PDriverNames.sqlsrv2008:
			case PDriverNames.sqlsrv: {
				return new Promise((resolve, reject) => {
					this.transactionInstance.commit(err => {
						if (err) {
							reject(err)
						} else {
							this._inTransaction = false
							resolve()
						}
					})
				})
			}
		}
	}

	rollbackTransaction(): Promise<void> {
		if (!this._inTransaction) throw new Error(`El conector no ha inicializado ninguna transacción`)
		switch (this.driver) {
			case PDriverNames.sqlsrv2008:
			case PDriverNames.sqlsrv: {
				return new Promise((resolve, reject) => {
					if (this.transactionInstance._aborted) {
						resolve()
					} else {
						this.transactionInstance.rollback(err => {
							if (err) {
								reject(err)
							} else {
								this._inTransaction = false
								resolve()
							}
						})
					}
				})
			}
		}
	}

	makeSelectCommand({ filter, from, select, where, having, joins, page, order, group, limit }: PSelectParams) {
		const isSqlServer = [PDriverNames.sqlsrv, PDriverNames.sqlsrv2008].includes(this.config.driver)

		const whereOrHavingDecoder = (target?: string | string[]) => {
			if (target == null) {
				return []
			} else if (target instanceof Array) {
				return target
			} else {
				return [target]
			}
		}

		/* WHERE */
		where = whereOrHavingDecoder(where)

		/* HAVING */
		having = whereOrHavingDecoder(having)

		/* Filtering */
		if (filter != null && filter.text) {
			if (!filter.fields.length) throw new Error(`La propiedad 'filter.fields' debe tener al menos un elemento`)
			const listOfWords = filter.text.replace(/\s+/g, ' ').split(' ').map(word => {
				switch (this.config.driver) {
					case PDriverNames.sqlsrv2008:
					case PDriverNames.sqlsrv:
						/* Se escapan los caracteres '%' y '\' en caso text los contenga, para no entrar en conflicto con el filtraje automático */
						return word.replace(/(%|\\)/g, char => `\\${char}`)
					default:
						return word
				}
			})
			const subWhere: string[] = []
			for (const field of filter.fields) {
				subWhere.push(listOfWords.map(word => {
					const omit = word[0] === '-'
					if (omit) word = word.substring(1)
					/* Se escapan los signos '%' */
					word = word.replace('%', '\\%')
					/* Si se está utilizando el caracter asterístico para realizar el filtraje, se utilizará la mecánica de comparación específica, la cual reemplazará el asterístico por el signo '%' y tendrá en cuenta la posición del mismo para realizar la comparación */
					if (word.match(/(?<!\*)\*(?!\*)/)) {
						/* Reemplaza los asterísticos por '%' siempre que no sea un doble asterístico */
						word = word.replace(/(?<!\*)\*(?!\*)/, '%').replace('**', '*')
					} else {
						word = `%${word}%`
					}

					return `lower(${field}) ${omit ? 'NOT' : ''} LIKE lower(${this.escape(word)})${isSqlServer ? ' collate SQL_Latin1_General_CP1_CI_AI' : ''} escape '\\'`
				}).join(' and '))
			}
			where.push(subWhere.join(' or '))
		}

		/* PAGE: Si se utiliza 'page', el valor de 'limit' se asignará automáticamente */
		if (page != null) {
			if (isSqlServer && !order) throw new Error(`La propiedad 'order' es requerida cuando se utiliza 'page' o 'limit' con el driver '${this.config.driver}'`)

			if (page < 0) throw new Error(`La propiedad 'page' debe ser mayor a cero`)
			if (Math.ceil(page) != page) throw new Error(`La propiedad 'page' debe ser un número entero`)

			limit = {
				offset: (page - 1) * this.config.rowsPerPage,
				count: this.config.rowsPerPage
			}
		}

		/* LIMIT */
		let limitString: string | undefined = undefined
		if (limit != null) {
			if (limit.offset < 0) throw new Error(`La propiedad 'limit.offset' debe ser mayor a cero`)
			if (Math.ceil(limit.offset) != limit.offset) throw new Error(`La propiedad 'limit.offset' debe ser un número entero`)
			if (limit.count < 0) throw new Error(`La propiedad 'limit.count' debe ser mayor a cero`)
			if (Math.ceil(limit.count) != limit.count) throw new Error(`La propiedad 'limit.count' debe ser un número entero`)

			limitString = (() => {
				switch (this.config.driver) {
					case PDriverNames.postgresql:
					case PDriverNames.mariadb:
						return `${limit.count} OFFSET ${limit.offset}`
					case PDriverNames.sqlsrv:
						return `OFFSET ${limit.offset} ROWS FETCH NEXT ${limit.count} ROWS ONLY`
					case PDriverNames.sqlsrv2008:
						return `${limit.offset} AND ${limit.offset + limit.count}`
				}
			})()
		}

		//Construye la sentencia.
		let query: string[] = [
			`SELECT ${select ?? '*'}`,
			`FROM ${from}`,
			joins ?? '',
			where.length ? /*sql*/`WHERE (${where.join(') AND (')})` : ''
		]

		//Si $page no está vacío, se paginan los resultados, caso contrario, se continúa la construcción de la sentencia poniendo al final las órdenes ORDER y GRUOP en caso no estén vacías.
		if (this.config.driver == PDriverNames.sqlsrv2008 && limitString) {
			query = [
				'SELECT',
				'FROM (',
				`\tSELECT`,
				`\t\t*`,
				`\t\trow_number() over (order by ${order}) as ___row_number`,
				`\tFROM (`,
				...query.map(q => `\t\t\t${q}`),
				group ? `\t\t\tGROUP BY ${group}` : '',
				having.length ? `\t\t\tHAVING (${having.join(') AND (')})` : '',
				`\t) AS SUBQUERY`,
				') AS FULLQUERY ',
				`WHERE ___row_number BETWEEN ${limit}`,
				`ORDER BY ${order}`
			]
		} else {
			query.push(
				group ? `GROUP BY ${group}` : '',
				having.length ? /*sql*/`HAVING (${having.join(') AND (')})` : '',
				order ? `ORDER BY ${order}` : ''
			)
			if (limitString) {
				if (this.config.driver == PDriverNames.sqlsrv) {
					query.push(limitString)
				} else {
					query.push(`LIMIT ${limitString}`)
				}
			}
		}

		return query.filter(q => q).join(`\n`)
	}

	makeCountCommand(params: PSelectParams) {
		const copyParams = { ...params }
		copyParams.select = `count(*) as "Count"`
		return this.makeSelectCommand(copyParams)
	}

	async count(params: PSelectParams) {
		return PUtils.Number.forceNumber((await this.queryOne(this.makeCountCommand(params)))?.Count)
	}

	async select<T = PTableRow>(params: PSelectParams, groupColumns?: boolean): Promise<PQueryResults<T>> {
		if (!(params.page >= 1)) {
			return await this.query<T>(this.makeSelectCommand({
				...params,
				page: undefined
			}), groupColumns)
		} else {
			const count = await this.count({
				...params,
				order: undefined,
				page: undefined
			})
			const limitPage = Math.max(Math.ceil(count / this.config.rowsPerPage), 1)
			let page = Math.floor(PUtils.Number.forceNumber(params.page))
			if (page == 0) {
				page = undefined
			} else {
				page = page > limitPage ? limitPage : page
			}
			const command = this.makeSelectCommand({
				...params,
				page: params.page > limitPage ? limitPage : params.page
			})
			if (!count) return {
				rows: [],
				rowsCount: 0,
				statement: command
			}
			const result = await this.query<T>(command, groupColumns)
			return {
				rows: result.rows,
				rowsCount: count,
				statement: command
			}
		}
	}

	async selectOne<T = PTableRow>(params: PSelectParams, groupColumns?: boolean) {
		return await this.queryOne<T>(this.makeSelectCommand(params), groupColumns)
	}

	async delete(table: string, where: string | string[]) {
		if (typeof where == 'string') where = [where]
		await this.query(`delete from ${table} where (${where.join(') and (')})`)
	}

	makeSaveCommand(table: string, ...params: PSaveParams[]) {
		const saveStatements: string[] = []
		for (const param of params) {
			const filteredPairs = Object.entries(param.values).filter(pair => pair[1] !== undefined)
			const pairs: [string, unknown][] = []
			for (const pair of filteredPairs) {
				try {
					pairs.push([`${this.fieldNameDelimiter}${pair[0]}${this.fieldNameDelimiter}`, this.escape(pair[1])])
				} catch {
					throw new Error(`Ocurrió un error al escapar el campo '${pair[0]}'`)
				}
			}
			if (param.where) {
				if (typeof param.where == 'string') param.where = [param.where]
				saveStatements.push(`UPDATE ${table} SET ${pairs.map(pair => `${pair[0]} = ${pair[1]}`).join(', ')} WHERE (${param.where.join(') AND (')});`)
			} else {
				const returning = this.config.driver == PDriverNames.postgresql && param.returning ? `returning "${param.returning}"` : ''
				saveStatements.push(`INSERT INTO ${table} (${pairs.map(pair => pair[0]).join(', ')}) VALUES (${pairs.map(pair => pair[1]).join(',')})${returning ? ` ${returning}` : ''};`)
			}
		}
		return saveStatements.join('\n')
	}

	makeSaveAndReturnCommand(table: string, ...params: PSaveParams[]): [string, string] {
		const saveStatements = this.makeSaveCommand(table, ...params)

		let getIDStatement: string
		switch (this.config.driver) {
			case PDriverNames.sqlsrv2008:
			case PDriverNames.sqlsrv:
				getIDStatement = /*sql*/`select @@identity as 'lastID'`
				break
			case PDriverNames.mariadb:
				getIDStatement = /*sql*/`select last_insert_id() as 'lastID'`
				break
			case PDriverNames.sqlite:
				getIDStatement = /*sql*/`select last_insert_rowid() as 'lastID'`
				break
		}
		return [saveStatements, getIDStatement + ';']
	}

	async save(table: string, ...params: PSaveParams[]): Promise<PQueryResults> {
		/* Obtiene los comandos */
		const commands = this.makeSaveAndReturnCommand(table, ...params)
		/* Ejecuta las sentencias de guardado */
		await this.exec(commands[0])
		/* Obtiene el último ID */
		const lastID = Number((await this.queryOne(commands[1]))?.lastID)
		return {
			rows: [],
			rowsCount: 0,
			statement: commands.join('\n'),
			lastID
		}
	}

	async batchSave(...params: PBatchSaveParams[]) {
		const command = params.map(param => this.makeSaveCommand(param.table, param)).join('\n')
		await this.exec(command)
	}

	async buildTable({ schema, table, comments, fields }: PBuildTableParams) {
		const config = this.config

		/* Obtiene información del schema, si no existe, lo crea crea */
		if (schema) {
			const schemaInformation = await (async () => {
				switch (this.config.driver) {
					case PDriverNames.sqlsrv2008:
					case PDriverNames.sqlsrv:
						return await this.queryOne(/*sql*/`
							select
								A0.name
							from sys.schemas A0
							where
								A0.name = '${schema}'
						`)
				}
			})()
			if (!schemaInformation) {
				switch (this.config.driver) {
					case PDriverNames.sqlsrv2008:
					case PDriverNames.sqlsrv:
						// await this.exec(/*sql*/`create schema [${schema}]`, true)
						await this.exec(/*sql*/`create schema [${schema}]`)
				}
			}
		}

		/* Información de la tabla */
		const tableInformation = await (async () => {
			switch (this.config.driver) {
				case PDriverNames.sqlsrv2008:
				case PDriverNames.sqlsrv:
					return await this.queryOne(/*sql*/`
						select
							A0.name,
							A1.value as comments
						from sys.objects A0
						left join sys.extended_properties A1 on A1.major_id = A0.object_id and A1.minor_id = 0
						inner join sys.schemas A2 on A2.schema_id = A0.schema_id
						where
							type = 'U'
							and A0.name = '${table}'
							and A2.name = '${schema ?? 'dbo'}'
					`)
				case PDriverNames.sqlite:
					return this.queryOne(/*sql*/`
						select
							A0.name
						from sqlite_schema A0
						where
							type = 'table'
							and A0.name = '${table}'
					`)
			}
		})()

		/* Validaciones para las definiciones de los campos */
		if (!Object.keys(fields).length) throw new Error(`La propiedad 'fields' debe tener definido al menos un campo`)
		for (const field in fields) {
			const fieldDefinition = fields[field]

			/* Homogeniza el tipo del campo de acuerdo al driver */
			fieldDefinition.type = (() => {
				if (config.driver == PDriverNames.sqlsrv && fieldDefinition.type == PFieldTypes.boolean) {
					return PFieldTypes.smallint
				} else if (config.driver == PDriverNames.sqlite && fieldDefinition.type == PFieldTypes.varchar) {
					return PFieldTypes.text
				} else if (config.driver == PDriverNames.sqlite && [PFieldTypes.bigint, PFieldTypes.smallint].includes(fieldDefinition.type)) {
					return PFieldTypes.int
				} else {
					return fieldDefinition.type
				}
			})()

			/* Valida la propiedad length */
			if (fieldDefinition.type == PFieldTypes.varchar && (!PUtils.Number.isInteger(fieldDefinition.length) || !PUtils.Number.isGreaterThan(fieldDefinition.length, 0))) {
				throw new Error(`Para la definición del campo '${field}', la propiedad 'length' debe ser un número entero positivo mayor a cero`)
			}

			if (fieldDefinition.type == PFieldTypes.decimal || fieldDefinition.type == PFieldTypes.numeric) {
				if (
					!PUtils.Number.isInteger(fieldDefinition.length[0])
					|| !PUtils.Number.isGreaterThan(fieldDefinition.length[0], 0)
					|| !PUtils.Number.isInteger(fieldDefinition.length[1])
					|| !PUtils.Number.isGreaterThan(fieldDefinition.length[1], 0)
				) {
					throw new Error(`Para la definición del campo '${field}', la propiedad 'length' debe ser un array de números enteros positivos`)
				}
			}

			/* Valida el valor de default según el tipo de campo */
			if (fieldDefinition.default !== undefined) {
				const errorMessageForType = `Para la definición del campo '${field}', la propiedad 'default' no corresponde al tipo del campo`
				switch (fieldDefinition.type) {
					case PFieldTypes.smallint:
					case PFieldTypes.bigint:
					case PFieldTypes.int:
						if (typeof fieldDefinition.default != 'number' || !PUtils.Number.isInteger(fieldDefinition.default)) throw new Error(errorMessageForType)
						break
					case PFieldTypes.float:
					case PFieldTypes.double:
					case PFieldTypes.decimal:
						if (typeof fieldDefinition.default != 'number') throw new Error(errorMessageForType)
						break
					case PFieldTypes.boolean:
						if (typeof fieldDefinition.default != 'boolean') throw new Error(errorMessageForType)
						fieldDefinition.default = fieldDefinition.default ? 1 : 0
						break
					case PFieldTypes.varchar:
						if (typeof fieldDefinition.default != 'string') throw new Error(errorMessageForType)
						break
					case PFieldTypes.date:
					case PFieldTypes.datetime:
						if (!(fieldDefinition.default instanceof Date)) throw new Error(errorMessageForType)
						switch (this.config.driver) {
							case PDriverNames.sqlsrv2008:
							case PDriverNames.sqlsrv:
								fieldDefinition.default = PUtils.Date.format(fieldDefinition.default, `'@y@mm@dd @hh:@ii:@ss'`)
								break
							default:
								fieldDefinition.default = PUtils.Date.format(fieldDefinition.default, `'@y-@mm-@dd @hh:@ii:@ss'`)
								break
						}
						break
				}
			}
		}

		if (tableInformation?.name) {
			const currentFields = await (async () => {
				switch (this.config.driver) {
					case PDriverNames.sqlsrv2008:
					case PDriverNames.sqlsrv:
						return await this.query(/*sql*/`
							select
								A0.name,
								A2.name as 'type',
								substring(A4.definition, 3, len(A4.definition) - 4) as 'default_value',
								A4.name as 'default_constraint_name',
								A3.value as 'comments',
								A0.max_length as 'length',
								A0.is_nullable
							from sys.all_columns A0
							inner join sys.tables A1 on A1.object_id = A0.object_id
							inner join sys.types A2 on A2.system_type_id = A0.system_type_id
							left join sys.extended_properties A3 on A3.major_id = A1.object_id and A3.minor_id = A0.column_id
							left join sys.default_constraints A4 on A4.parent_object_id = A1.object_id and A4.parent_column_id = A0.column_id
							left join sys.schemas A5 on A5.schema_id = A1.schema_id
							where
								A1.name = '${table}'
								${schema ? `and A5.name = '${schema}'` : ''}
						`)
					case PDriverNames.sqlite:
						return this.query(/*sql*/`
							select
								name,
								type,
								dflt_value as 'default_value'
							from pragma_table_info('${table}')
						`)
				}
			})()

			const recordsCount = await this.count({ from: `${schema ? `[${schema}].` : ''}[${table}]` })

			for (const field in fields) {
				const fieldDefinition: PFieldDefinition = fields[field] as PFieldDefinition

				/* Verifica si el campo ya existe */
				const currentField = PUtils.Array.queryOne(currentFields.rows, (specification: PTableRow) => (specification.name as string).toLowerCase() == field.toLowerCase())

				if (!currentField) {
					/* Si la columna no existe, intentará crearla */
					if (config.driver != PDriverNames.sqlite && fieldDefinition.primaryKey == true) {
						/* Validación que impedirá insertar una columna 'id' si existen registros en la tabla */
						if (recordsCount) throw new Error(`La tabla '${schema ? `${schema}.` : ''}${table}' cuenta con registros. No se puede agregar el ID '${field}'`)
						switch (config.driver) {
							case PDriverNames.sqlsrv2008:
							case PDriverNames.sqlsrv:
								//ALTER TABLE BD_EXTENDS.Comprobantes.DocumentosArchivos ADD Column1 int IDENTITY(0,1) NULL;
								await this.queryOne(/*sql*/ `
									alter table ${schema ? `"${schema}".` : ''}"${table}" add ${field} ${this.getTypeForCommand(fieldDefinition)} ${('autoincrement' in fieldDefinition && fieldDefinition.autoincrement) ? `identity(0, 1)` : ''} not null;
									alter table ${schema ? `"${schema}".` : ''}"${table}" add constraint "${table}_pk" primary key (${field});
								`)
								break
						}
					} else {
						/* Si ya existen registros, no se podrá crear una columna sin haber especificado un valor por defecto */
						if (recordsCount && fieldDefinition.notNull && fieldDefinition.default == null) {
							throw new Error(`La tabla '${table}' cuenta con registros. No se puede agregar la columna '${field}' como 'notNull' sin haber especificado un valor en la propiedad 'default'`)
						}
						switch (config.driver) {
							case PDriverNames.sqlsrv2008:
							case PDriverNames.sqlsrv:
								await this.queryOne(/*sql*/ `
									alter table ${schema ? `[${schema}].` : ''}[${table}] add
										${field} 
										${this.getTypeForCommand(fieldDefinition)}
										${fieldDefinition.notNull ? "not null" : 'null'}
										${fieldDefinition.default != undefined ? `default ${this.escape(fieldDefinition.default)}` : ''}
								`)
								break
							case PDriverNames.sqlite:
								await this.queryOne(/*sql*/`
									ALTER TABLE ${schema ? `[${schema}].` : ''}[${table}] ADD
										${field} 
										${this.getTypeForCommand(fieldDefinition)} DEFAULT (5);
										${fieldDefinition.default != undefined ? `default ${this.escape(fieldDefinition.default)}` : ''}
								`)
								break
						}

						/* Se actualiza el valor por defecto definido en la columna */
						// if (fieldDefinition.default !== undefined) {
						// 	switch (this.config.driver) {
						// 		case DriverNames.sqlsrv2008:
						// 		case DriverNames.sqlsrv:
						// 			if (currentField.default_constraint_name) await this.query(/*sql*/`alter table "${table}" drop constraint "${currentField.default_constraint_name}"`)
						// 			await this.query(/*sql*/`alter table "${table}" add default ${fieldDefinition.default} for "${field}"`)
						// 			break
						// 	}
						// }
					}
				} else {
					/* Si la columna ya existe, intentará actualizarla siempre que no se requiera que sea de tipo id */
					if (!fieldDefinition.primaryKey) {
						const currentType = (currentField.type as string).toLowerCase()

						/* Actualiza el tipo de columna en caso encuentre diferencia */
						if (
							currentType != fieldDefinition.type.toString()
							|| (currentType == 'varchar' && fieldDefinition.type == PFieldTypes.varchar && (currentField.length as number) < fieldDefinition.length)
						) {
							switch (this.config.driver) {
								case PDriverNames.sqlsrv2008:
								case PDriverNames.sqlsrv:
									await this.queryOne(/*sql*/`alter table "${schema ?? 'dbo'}"."${table}" alter column "${field}" ${this.getTypeForCommand(fieldDefinition)}`)
									break
							}
						}

						/* Actualiza los comentarios */
						if ((currentField.comments ?? '') != (fieldDefinition.comments ?? '')) {
							switch (this.config.driver) {
								case PDriverNames.sqlsrv2008:
								case PDriverNames.sqlsrv:
									await this.queryOne(/*sql*/`
										exec sys.${!currentField.comments ? 'sp_addextendedproperty' : 'sp_updateextendedproperty'} 'MS_Description', N'${fieldDefinition.comments}',
											'schema', N'${schema ?? 'dbo'}', 'table', N'${table}', 'column', N'${field}'
									`)
									break
							}
						}

						/* Actualiza el valor por defecto */
						if (fieldDefinition.default != currentField.default_value) {
							switch (this.config.driver) {
								case PDriverNames.sqlsrv2008:
								case PDriverNames.sqlsrv:
									if (currentField.default_constraint_name) await this.query(/*sql*/`alter table "${schema ?? 'dbo'}"."${table}" drop constraint "${currentField.default_constraint_name}"`)
									if (fieldDefinition.default != null) await this.query(/*sql*/`alter table "${schema ?? 'dbo'}"."${table}" add default ${this.escape(fieldDefinition.default)} for "${field}"`)
									break
							}
						}

						/* Si el nombre es diferente debido a las mayúsculas y minúsculas, lo renombra */
						if ((currentField.name as string).toLowerCase() != field.toLowerCase()) {
							switch (this.config.driver) {
								case PDriverNames.sqlsrv2008:
								case PDriverNames.sqlsrv:
									await this.queryOne(/*sql*/`
										alter table "${schema ?? 'dbo'}"."${table}" change column "${currentField.name}" "${field}"
											${this.getTypeForCommand(fieldDefinition)}
											${fieldDefinition.default !== undefined ? `default ${fieldDefinition.default}` : ''}
											${fieldDefinition.notNull ? "not null" : "null"} 
											comment ${fieldDefinition.comments ? this.escape(fieldDefinition.comments) : "''"}
									`)
									break
							}
						}

						/* Si es notNull */
						if (
							((currentField.is_nullable as number) && fieldDefinition.notNull)
							|| (!(currentField.is_nullable as number) && !fieldDefinition.notNull)
						) {
							switch (this.config.driver) {
								case PDriverNames.sqlsrv2008:
								case PDriverNames.sqlsrv:
									await this.queryOne(/*sql*/`
										ALTER TABLE "${schema ?? 'dbo'}"."${table}" ALTER COLUMN "${field}" ${this.getTypeForCommand(fieldDefinition)} ${fieldDefinition.notNull ? 'NOT' : ''} NULL
									`)
									break
							}
						}
					}
				}
			}
		} else {
			/* En caso no exista la tabla, intentará crearla */
			const fieldsCommands: string[] = []
			const constraints: string[] = []
			for (const field in fields) {
				const fieldDefinition: PFieldDefinition = fields[field] as PFieldDefinition
				if (fieldDefinition.primaryKey) {
					switch (config.driver) {
						case PDriverNames.sqlsrv2008:
						case PDriverNames.sqlsrv: {
							const identity = ('autoincrement' in fieldDefinition && fieldDefinition.autoincrement) ? 'identity(1,1)' : ''
							fieldsCommands.push(`${field} ${fieldDefinition.type} ${identity} not null`)
							constraints.push(/*sql*/`constraint ${table}_${field}_pk primary key clustered ([${field}])`)
							break
						}
						case PDriverNames.sqlite:
							fieldsCommands.push(`${field} ${this.getTypeForCommand(fieldDefinition)} PRIMARY KEY ${[PFieldTypes.int].includes(fieldDefinition.type) ? 'AUTOINCREMENT' : ''}`)
							break
					}
				} else {
					fieldsCommands.push([
						`"${field}"`,
						this.getTypeForCommand(fieldDefinition),
						fieldDefinition.notNull ? 'not null' : 'null',
						fieldDefinition.default !== undefined ? `default ${fieldDefinition.default}` : ''
					].join(' '))
				}
			}
			/* Ejecuta la sentencia */
			switch (this.config.driver) {
				case PDriverNames.sqlsrv2008:
				case PDriverNames.sqlsrv:
					await this.queryOne(/*sql*/ `
						create table ${schema ? `[${schema}].` : ''}[${table}] (
							${fieldsCommands.join(",\n")}${constraints.length ? ',' : ''}
							${constraints.join(",\n")}
						)
					`)
					break
				case PDriverNames.sqlite:
					await this.queryOne(/*sql*/ `
						create table ${table} (
							${fieldsCommands.join(",\n")}${constraints.length ? ',' : ''}
							${constraints.join(",\n")}
						)
					`)
					break
			}
		}

		/* Asignación de los comentarios a la tabla */
		switch (this.config.driver) {
			case PDriverNames.sqlsrv2008:
			case PDriverNames.sqlsrv:
				await this.queryOne(/*sql*/`EXEC sys.${tableInformation?.comments == null ? 'sp_addextendedproperty' : 'sp_updateextendedproperty'} 'MS_Description', N${this.escape(comments)}, 'schema', N'${schema ? schema : 'dbo'}', 'table', N${this.escape(table)}`)
				break
		}

		/* Construcción de las llaves primarias para varios drivers */
		if (config.driver != PDriverNames.sqlite) {
			/* Obtiene las limitaciones */
			const constraints = await (async () => {
				switch (this.config.driver) {
					case PDriverNames.sqlsrv2008:
					case PDriverNames.sqlsrv:
						return await this.query(/*sql*/`
							select * 
							from sys.objects A0
							inner join sys.objects A1 on A1.object_id = A0.parent_object_id
							inner join sys.schemas A2 on A2.schema_id = A1.schema_id
							where
								A1.name = '${table}'
								and A0.type = 'PK'
								${schema ? `and A2.name = '${schema}'` : ''}
						`)
				}
			})()

			for (const field in fields) {
				const fieldDefinition = fields[field]
				/* Crea las llaves primaria cuando existe un ID */
				if (fieldDefinition.primaryKey && !constraints?.rowsCount) {
					switch (this.config.driver) {
						case PDriverNames.sqlsrv2008:
						case PDriverNames.sqlsrv:
							await this.queryOne(/*sql*/`alter table [${table}] add constraint "${field}_pk" primary key clustered ("${field}")`)
							break
					}
				}
			}
		}
	}

	async buildForeignKeys({ schema, table, fields }: PBuildForeignKeysParams) {
		const config = this.config
		if (config.driver == PDriverNames.sqlite) throw new Error(`No es posible crear llaves foráneas en bases de datos de tipo 'sqlite'`)

		/* Obtiene las limitaciones referenciales */
		const referentialConstraints = await (async () => {
			switch (this.config.driver) {
				case PDriverNames.sqlsrv2008:
				case PDriverNames.sqlsrv:
					return await this.query(/*sql*/`
						select
							/* Información de la tabla que tiene la referencia */
							A1.name as table_name,
							A2.name as column_name,
							/* Información de la tabla referenciada */
							A3.name as reference_table,
							A4.name as reference_column
						from sys.foreign_key_columns A0
						inner join sys.tables A1 on A1.object_id = A0.parent_object_id
						inner join sys.columns A2 on A2.object_id = A1.object_id and A2.column_id = A0.parent_column_id
						inner join sys.tables A3 on A3.object_id = A0.referenced_object_id
						inner join sys.columns A4 on A4.object_id = A3.object_id and A4.column_id = A0.referenced_column_id
						where 
							A1.name = '${table}'
					`)
			}
		})()

		/* Adición de atributos adicionales */
		for (const field in fields) {
			const fieldDefinition = fields[field]
			/* Respecto al foreignKey */
			if (fieldDefinition.foreignKey) {
				/* Validación del foreignKey */
				const foreignKey = fieldDefinition.foreignKey
				const referentialConstraint = PUtils.Array.queryOne(referentialConstraints.rows, { table_name: table, column_name: field, reference_table: foreignKey.table, reference_column: foreignKey.field })
				/* Si se ha definido una llave foránea, se crea */
				if (!referentialConstraint) {
					try {
						switch (this.config.driver) {
							case PDriverNames.sqlsrv2008:
							case PDriverNames.sqlsrv:
								await this.queryOne(/*sql*/`ALTER TABLE "${schema ?? 'dbo'}"."${table}" ADD CONSTRAINT "fk_${schema ?? 'dbo'}_${table}_${field}" FOREIGN KEY ("${field}") REFERENCES "${foreignKey.schema ?? 'dbo'}"."${foreignKey.table}"("${foreignKey.field}")`)
								break
						}
					} catch (err) {
						throw new Error(`No se pudo crear el 'foreignKey' '${table}.fk_${table}_${field}' por lo siguiente:\n${err.message}`)
					}
				}
			} else {
				/* Si no se ha definido una llave foránea, se verifica si existe una que amarre al mismo campo con la misma tabla y se elimina */
				const referentialConstraint = PUtils.Array.query(referentialConstraints.rows, { column_name: field, table_name: table })
				if (referentialConstraint) {
					for (const rc of referentialConstraint) {
						try {
							switch (this.config.driver) {
								case PDriverNames.sqlsrv2008:
								case PDriverNames.sqlsrv:
									await this.queryOne(/*sql*/`ALTER TABLE ${schema ? `"${schema}".` : ''}."${table}" drop CONSTRAINT "${rc.CONSTRAINT_NAME}"`)
									break
							}
						} catch (err) {
							throw new Error(`No se pudo eliminar el 'foreignKey' de nombre '${rc.constraint_name}' por lo siguiente:\n${err.message}`)
						}
					}
				}
			}
		}
	}

	private getTypeForCommand = (fieldDefinition: PFieldDefinition) => {
		switch (fieldDefinition.type) {
			case PFieldTypes.varchar:
				return `varchar(${fieldDefinition.length})`
			case PFieldTypes.numeric:
			case PFieldTypes.decimal:
				return `${fieldDefinition.type}(${fieldDefinition.length})`
			case PFieldTypes.text:
				return fieldDefinition.type.toString()
			case PFieldTypes.int:
			case PFieldTypes.bigint:
			case PFieldTypes.smallint:
				if (this.config.driver == PDriverNames.sqlite) {
					return `integer`
				} else {
					return fieldDefinition.type.toString()
				}
			default:
				return fieldDefinition.type.toString()
		}
	}
}