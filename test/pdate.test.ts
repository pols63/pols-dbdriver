import { PDBDriver, PDriverNames } from '../src/index'

const runTest = async () => {
	const db = new PDBDriver({
		host: 'localhost',
		driver: PDriverNames.sqlsrv,
		user: 'sa',
		password: 'password',
		rowsPerPage: 50,
	})

	// Simular conexión
	db['_connected'] = true

	let capturedSql = ''
	const capturedInputs: Record<string, any> = {}

	db['engine'] = {
		request: () => {
			return {
				input: (name: string, value: any) => {
					capturedInputs[name] = value
				},
				query: async (sql: string) => {
					capturedSql = sql
					return { recordset: [] }
				}
			}
		}
	} as any

	const mockPDate = {
		engine: new Date('2026-07-06T11:00:56.000Z')
	}

	const expectedEscapedDate = db.escape(mockPDate)

	const sql = `EXEC MyProcedure @fechaInicial, @fechaFinal, @primitiveParam, NULL, NULL`
	await db.query(sql, {
		fechaInicial: mockPDate,
		fechaFinal: mockPDate,
		primitiveParam: 123
	})

	console.log('--- TEST RESULTS ---')
	console.log('Captured SQL:', capturedSql)
	console.log('Captured Inputs:', capturedInputs)
	console.log('Expected Escaped Date:', expectedEscapedDate)

	// Verificaciones
	const hasFechaInicial = capturedSql.includes(expectedEscapedDate)
	const isPrimitiveParamInput = capturedInputs['primitiveParam'] === 123
	const isFechaInicialInput = 'fechaInicial' in capturedInputs

	console.log('Has fechaInicial in SQL:', hasFechaInicial)
	console.log('Is primitiveParam registered as native input:', isPrimitiveParamInput)
	console.log('Is fechaInicial NOT registered as native input (good):', !isFechaInicialInput)

	if (hasFechaInicial && isPrimitiveParamInput && !isFechaInicialInput) {
		console.log('TEST PASSED!')
	} else {
		console.error('TEST FAILED!')
		process.exit(1)
	}
}

runTest().catch(err => {
	console.error(err)
	process.exit(1)
})
