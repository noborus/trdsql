package trdsql

import "log"

type debugT bool

var debug = debugT(false)

func EnableDebug() {
	debug = true
}

func (d debugT) Printf(format string, args ...interface{}) {
	if d {
		log.Printf(format, args...)
	}
}
