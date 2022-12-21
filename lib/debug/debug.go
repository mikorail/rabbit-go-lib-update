package debug

import "log"

// Debug debug log flag
var Debug bool

func Print(args ...interface{}) {
	if !Debug {
		return
	}
	log.Print(args...)
}

func Printf(format string, args ...interface{}) {
	if !Debug {
		return
	}
	log.Printf(format, args...)
}
