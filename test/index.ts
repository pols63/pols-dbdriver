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

	const resultados = await db.query(`select top 5 ItemCode, ItemName from SBO_ROM_SAC..OITM`)
}

run().then(() => {
	process.exit()
}).catch(error => {
	console.error(error)
	process.exit(1)
})