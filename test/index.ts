import { PDBDriver, PDriverNames } from './../src/index'

const run = async () => {
	const db = new PDBDriver({
		host: '10.10.0.22',
		driver: PDriverNames.sqlsrv,
		user: 'sa',
		password: '10078612369Lore',
		rowsPerPage: 50,
	})
	await db.connect()

	const resultados = await db.query(`select top 5 TransId, Memo from SBO_ROM_SAC..OJDT where RefDate = $refDate`, {
		refDate: new Date('2025-07-21 00:00:00')
	})
	console.log(resultados)
}

run().then(() => {
	process.exit()
}).catch(error => {
	console.error(error)
	process.exit(1)
})