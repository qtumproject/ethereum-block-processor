package log

import (
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	logger      *logrus.Logger
	loggerError error
	loggerOnce  sync.Once
)

func GetLogger(opts ...Option) (*logrus.Logger, error) {
	loggerOnce.Do(func() {
		logger, loggerError = createNewLogger(opts...)
	})
	return logger, loggerError
}

func createNewLogger(opts ...Option) (*logrus.Logger, error) {
	logger = logrus.New()
	formatter := &logrus.TextFormatter{
		TimestampFormat: "02-01-2006 15:04:05",
		FullTimestamp:   true,
		ForceColors:     true,
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			return fmt.Sprintf("%s \t ", formatFilePath(f.Function)), fmt.Sprintf(" %s:%d \t", formatFilePath(f.File), f.Line)
		},
	}

	logger.SetFormatter(formatter)

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
