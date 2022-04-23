package log

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	// "github.com/sirupsen/logrus/hooks/writer"
)

var (
	logger      *logrus.Logger
	loggerError error
	loggerOnce  sync.Once
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

func GetLogger(opts ...Option) (*logrus.Logger, error) {
	loggerOnce.Do(func() {
		logger, loggerError = createNewLogger(opts...)
	})
	return logger, loggerError
}

func createNewLogger(opts ...Option) (*logrus.Logger, error) {
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

	for _, opt := range opts {
		if err := opt(logger); err != nil {
			return nil, err
		}
	}

	return logger, nil
}

func formatFilePath(path string) string {
	arr := strings.Split(path, "/")
	return arr[len(arr)-1]
}

func DecorateRuntimeContext(logger *logrus.Entry) *logrus.Entry {
	if pc, file, line, ok := runtime.Caller(1); ok {
		fName := runtime.FuncForPC(pc).Name()
		return logger.WithField("file", file).WithField("line", line).WithField("func", fName)
	} else {
		return logger
	}
}

type Option func(logger *logrus.Logger) error

func WithDebugLevel(debug bool) Option {
	return func(logger *logrus.Logger) error {
		if debug {
			logger.SetLevel(logrus.DebugLevel)
		}
		return nil
	}
}

func WithOutput(output io.Writer) Option {
	return func(logger *logrus.Logger) error {
		logger.SetOutput(output)
		return nil
	}
}
