import { PDate } from '../src/pdate'

const fecha = new PDate('2024-10-28 23:56:00')
console.log(fecha.toString())

const fecha2 = new PDate('2024-10-28 23:57:10.056')
console.log(fecha2.toString())

console.log(fecha2.minutesDifference(fecha))