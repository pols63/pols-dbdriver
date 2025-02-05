import { PDate } from '../src'
import { PLogger } from '../src/plogger'

const logger = new PLogger({
	showIn: {
		file: true
	},
	fileName: () => {
		const now = new PDate
		return `LOGS ${now.toString('@y-@mm-@dd')}.txt`
	},
	destinationPath: __dirname
})
logger.system({ label: 'UNO', description: 'uno' })