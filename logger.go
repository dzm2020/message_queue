package queue

import "log"

// Logger 定义可注入日志能力。
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

func defaultLogger() Logger {
	//return &stdLogger{base: log.Default()}
	return &noopLogger{}
}

type stdLogger struct {
	base *log.Logger
}

func (l *stdLogger) Debugf(format string, args ...any) {
	l.base.Printf("[DEBUG] "+format, args...)
}

func (l *stdLogger) Infof(format string, args ...any) {
	l.base.Printf("[INFO] "+format, args...)
}

func (l *stdLogger) Warnf(format string, args ...any) {
	l.base.Printf("[WARN] "+format, args...)
}

func (l *stdLogger) Errorf(format string, args ...any) {
	l.base.Printf("[ERROR] "+format, args...)
}

type noopLogger struct{}

func (noopLogger) Debugf(string, ...any) {}
func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Warnf(string, ...any)  {}
func (noopLogger) Errorf(string, ...any) {}
