package log

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	// "github.com/sirupsen/logrus/hooks/writer"
)

var (
	logger     *logrus.Logger
	DebugLevel *bool
)

type CustomHook struct {
}

func (h *CustomHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.ErrorLevel, logrus.DebugLevel, logrus.FatalLevel, logrus.PanicLevel}
}

func (h *CustomHook) Fire(e *logrus.Entry) error {
	// if pc, file, line, ok := runtime.Caller(1); ok {
	// 	fName := runtime.FuncForPC(pc).Name()
	// 	e.Data["file"] = file
	// 	e.Data["line"] = line
	// 	e.Data["func"] = fName
	// }
	return nil
}

var lock = &sync.Mutex{}
var loggerOnce sync.Once

func GetLogger() *logrus.Logger {
	loggerOnce.Do(func() {
		logger = logrus.New()
		if _, err := os.Stat("./error.log"); err == nil {
			os.Remove("./error.log")
		}
		if _, err := os.Stat("./output.log"); err == nil {
			os.Remove("./output.log")
		}
		pathMap := lfshook.PathMap{
			logrus.ErrorLevel: "./error.log",
			logrus.DebugLevel: "./output.log",
			logrus.InfoLevel:  "./output.log",
			logrus.WarnLevel:  "./output.log",
		}
		logger.Hooks.Add(lfshook.NewHook(
			pathMap,
			&logrus.JSONFormatter{
				TimestampFormat: "02-01-2006 15:04:05", // the "time" field configuratiom
				CallerPrettyfier: func(f *runtime.Frame) (string, string) {
					return fmt.Sprintf("%s ", formatFilePath(f.Function)), fmt.Sprintf(" %s:%d ", formatFilePath(f.File), f.Line)
				},
			},
		))
		logger.SetOutput(os.Stdout)
		if _, err := os.Stat("./output.log"); err == nil {
			os.Remove("./output.log")
		}
		if *DebugLevel {
			logger.SetLevel(logrus.DebugLevel)
		}

		formatter := &logrus.TextFormatter{
			TimestampFormat: "02-01-2006 15:04:05", // the "time" field configuratiom
			FullTimestamp:   true,
			CallerPrettyfier: func(f *runtime.Frame) (string, string) {
				return fmt.Sprintf("%s \t ", formatFilePath(f.Function)), fmt.Sprintf(" %s:%d \t", formatFilePath(f.File), f.Line)
			},
		}
		logger.SetFormatter(formatter)
		logger.SetReportCaller(true)
		logger.AddHook(&CustomHook{})
	})
	return logger
}

func GetLoggerMutex() *logrus.Logger {
	if logger == nil {
		lock.Lock()
		defer lock.Unlock()
		if logger == nil {
			logger = logrus.New()
			if _, err := os.Stat("./error.log"); err == nil {
				os.Remove("./error.log")
			}
			pathMap := lfshook.PathMap{
				logrus.ErrorLevel: "./error.log",
			}
			logger.Hooks.Add(lfshook.NewHook(
				pathMap,
				&logrus.JSONFormatter{
					TimestampFormat: "02-01-2006 15:04:05", // the "time" field configuratiom
					CallerPrettyfier: func(f *runtime.Frame) (string, string) {
						return fmt.Sprintf("%s ", formatFilePath(f.Function)), fmt.Sprintf(" %s:%d ", formatFilePath(f.File), f.Line)
					},
				},
			))
			// logger.SetOutput(ioutil.Discard)
			// logger.AddHook(&writer.Hook{ // Send logs with level higher than warning to stderr
			// 	Writer: os.Stderr,
			// 	LogLevels: []logrus.Level{
			// 		logrus.PanicLevel,
			// 		logrus.FatalLevel,
			// 		logrus.ErrorLevel,
			// 		logrus.WarnLevel,
			// 	},
			// })
			// logger.AddHook(&writer.Hook{ // Send info and debug logs to stdout
			// 	Writer: os.Stdout,
			// 	LogLevels: []logrus.Level{
			// 		logrus.InfoLevel,
			// 		logrus.DebugLevel,
			// 	},
			// })
			logger.SetOutput(os.Stdout)

			if *DebugLevel {
				logger.SetLevel(logrus.DebugLevel)
			}

			formatter := &logrus.TextFormatter{
				TimestampFormat: "02-01-2006 15:04:05", // the "time" field configuratiom
				FullTimestamp:   true,
				CallerPrettyfier: func(f *runtime.Frame) (string, string) {
					return fmt.Sprintf("%s \t ", formatFilePath(f.Function)), fmt.Sprintf(" %s:%d \t", formatFilePath(f.File), f.Line)
				},
			}
			logger.SetFormatter(formatter)
			logger.SetReportCaller(true)
			logger.AddHook(&CustomHook{})
			return logger
		}
	}
	return logger
}

func formatFilePath(path string) string {
	arr := strings.Split(path, "/")
	return arr[len(arr)-1]
}

// func GetFileLogger(filename string) *logrus.Logger {
// 	f, err := os.OpenFile(filename, os.O_WRONLY | os.O_CREATE, 0755)
// 	if err != nil {
// 	    panic(err)
// 	}
// 	logrus.SetOutput(f)

// }

func DecorateRuntimeContext(logger *logrus.Entry) *logrus.Entry {
	if pc, file, line, ok := runtime.Caller(1); ok {
		fName := runtime.FuncForPC(pc).Name()
		return logger.WithField("file", file).WithField("line", line).WithField("func", fName)
	} else {
		return logger
	}
}
