package logpkg

import (
	"fmt"
	"io"
	"time"
)

func nowFunc() time.Time {
	return time.Now()
}

type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

var logLevels = []string{
	"DEBUG",
	"INFO",
	"WARN",
	"ERROR",
}

func (ll LogLevel) String() string {
	return logLevels[ll]
}

type Logger struct {
	outWriter io.Writer
	nowFunc   func() time.Time
	logLevel  LogLevel
}

func NewLogger(writer io.Writer, logLevel LogLevel) *Logger {
	return &Logger{writer, nowFunc, logLevel}
}

func (l *Logger) Debug(message string, args ...interface{}) {
	l.log(LogLevelDebug, message, args...)
}

func (l *Logger) Info(message string, args ...interface{}) {
	l.log(LogLevelInfo, message, args...)
}

func (l *Logger) Warn(message string, args ...interface{}) {
	l.log(LogLevelWarn, message, args...)
}

func (l *Logger) Error(message string, args ...interface{}) {
	l.log(LogLevelError, message, args...)
}

func (l *Logger) log(level LogLevel, message string, args ...interface{}) {
	if level < l.logLevel {
		return
	}
	text := fmt.Sprintf(message, args...)
	l.outWriter.Write([]byte(fmt.Sprintf("%s %s: %s\n", l.nowFunc().Format("2006/01/02 15:04:05"), level, text)))
}
