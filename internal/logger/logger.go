package logger

import (
	"log/slog"
	"os"
)

// ----------------------------------------------------------------
// LOGGER
// ----------------------------------------------------------------

type LevelT int

const (
	DEBUG LevelT = LevelT(slog.LevelDebug)
	INFO  LevelT = LevelT(slog.LevelInfo)
	WARN  LevelT = LevelT(slog.LevelWarn)
	ERROR LevelT = LevelT(slog.LevelError)

	extraFieldName = "extra"
)

type ExtraFieldsT map[string]any

func (e ExtraFieldsT) Set(key string, val any) {
	e[key] = val
}

func (e ExtraFieldsT) Del(key string) {
	delete(e, key)
}

type LoggerT struct {
	logger *slog.Logger
}

func NewLogger(level LevelT) (logger LoggerT) {
	opts := &slog.HandlerOptions{
		AddSource: false,
		Level:     slog.Level(level),
	}
	jsonHandler := slog.NewJSONHandler(os.Stdout, opts)
	logger.logger = slog.New(jsonHandler)

	return logger
}

func (l *LoggerT) Debug(msg string, extra ExtraFieldsT) {
	if extra == nil {
		extra = make(ExtraFieldsT)
	}
	l.logger.Debug(msg, extraFieldName, extra)
}

func (l *LoggerT) Info(msg string, extra ExtraFieldsT) {
	if extra == nil {
		extra = make(ExtraFieldsT)
	}
	l.logger.Info(msg, extraFieldName, extra)
}

func (l *LoggerT) Warn(msg string, extra ExtraFieldsT, err error) {
	if extra == nil {
		extra = make(ExtraFieldsT)
	}
	if err != nil {
		extra.Set("error", err.Error())
	}
	l.logger.Warn(msg, extraFieldName, extra)
	extra.Del("error")
}

func (l *LoggerT) Error(msg string, extra ExtraFieldsT, err error, stopExecution bool) {
	if extra == nil {
		extra = make(ExtraFieldsT)
	}
	if err != nil {
		extra.Set("error", err.Error())
	}
	l.logger.Error(msg, extraFieldName, extra)
	extra.Del("error")

	if stopExecution {
		os.Exit(1)
	}
}

func GetLevel(levelStr string) (l LevelT) {
	levelMap := map[string]LevelT{
		"debug": DEBUG,
		"info":  INFO,
		"warn":  WARN,
		"error": ERROR,
	}

	l, ok := levelMap[levelStr]
	if !ok {
		l = DEBUG
	}

	return l
}

// Dummy logger with {loggers.Standard loggers.Advanced} implementation

type DummyLogger struct{}

func (l DummyLogger) Fatal(args ...any)                 {}
func (l DummyLogger) Fatalf(format string, args ...any) {}
func (l DummyLogger) Fatalln(args ...any)               {}
func (l DummyLogger) Panic(args ...any)                 {}
func (l DummyLogger) Panicf(format string, args ...any) {}
func (l DummyLogger) Panicln(args ...any)               {}
func (l DummyLogger) Print(args ...any)                 {}
func (l DummyLogger) Printf(format string, args ...any) {}
func (l DummyLogger) Println(args ...any)               {}

func (l DummyLogger) Debug(args ...any)                 {}
func (l DummyLogger) Debugf(format string, args ...any) {}
func (l DummyLogger) Debugln(args ...any)               {}
func (l DummyLogger) Error(args ...any)                 {}
func (l DummyLogger) Errorf(format string, args ...any) {}
func (l DummyLogger) Errorln(args ...any)               {}
func (l DummyLogger) Info(args ...any)                  {}
func (l DummyLogger) Infof(format string, args ...any)  {}
func (l DummyLogger) Infoln(args ...any)                {}
func (l DummyLogger) Warn(args ...any)                  {}
func (l DummyLogger) Warnf(format string, args ...any)  {}
func (l DummyLogger) Warnln(args ...any)                {}
