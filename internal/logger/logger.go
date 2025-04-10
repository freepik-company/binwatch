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

func (l *LoggerT) Error(msg string, extra ExtraFieldsT, err error) {
	if extra == nil {
		extra = make(ExtraFieldsT)
	}
	if err != nil {
		extra.Set("error", err.Error())
	}
	l.logger.Error(msg, extraFieldName, extra)
	extra.Del("error")
}

func (l *LoggerT) Fatal(msg string, extra ExtraFieldsT, err error) {
	if extra == nil {
		extra = make(ExtraFieldsT)
	}
	if err != nil {
		extra.Set("error", err.Error())
	}
	l.logger.Error(msg, extraFieldName, extra)
	os.Exit(1)
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
