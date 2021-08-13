package bloom

import (
	"log"
)

type Logger func(v ...interface{})

func StdLogger(logger *log.Logger) Logger {
	if logger == nil {
		logger = log.Default()
	}
	return func(v ...interface{}) {
		logger.Println(v...)
	}
}
