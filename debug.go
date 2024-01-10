package trdsql

import "log"

// debugT is a type of debug flag.
type debugT bool

// debug is a flag for detailed output.
var debug = debugT(false)

// EnableDebug is enable verbose output for debug.
func EnableDebug() {
	debug = true
}

func (d debugT) Printf(format string, args ...any) {
	if d {
		log.Printf(format, args...)
	}
}
