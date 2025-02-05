import { PUtils } from '../src/index'

class MyClass {
	toString() {
		return 'jeje'
	}
}

const mv = {
	a: [{
		b: 3
	}, {
		c: 4
	}, {
		d: new MyClass
	}]
}

let i = 0
const cloned = PUtils.JSON.clone(mv, e => {
	console.log(i++)
	if (e instanceof MyClass) {
		return new Date
	} else {
		return e
	}
})

console.log(PUtils.JSON.stringify(cloned))

console.log(JSON.stringify(cloned))