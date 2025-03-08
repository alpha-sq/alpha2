package crawler

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"gorm.io/gorm/logger"
)

// ZeroLogger is a custom GORM logger that uses zerolog
type ZeroLogger struct {
	log zerolog.Logger
}

// LogMode sets the log level (required for GORM's logger interface)
func (zl *ZeroLogger) LogMode(level logger.LogLevel) logger.Interface {
	newLogger := zl.log.Level(convertGormLevel(level))
	return &ZeroLogger{log: newLogger}
}

// Info logs general information
func (zl *ZeroLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	zl.log.Info().Msgf(msg, data...)
}

// Warn logs warnings
func (zl *ZeroLogger) Warn(ctx context.Context, msg string, data ...any) {
	zl.log.Warn().Msgf(msg, data...)
}

// Error logs errors
func (zl *ZeroLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	zl.log.Error().Msgf(msg, data...)
}

// Trace logs SQL queries with execution time
func (zl *ZeroLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	sql, rows := fc()
	elapsed := time.Since(begin)

	event := zl.log.Info().Str("sql", sql).Dur("duration", elapsed).Int64("rows", rows)
	if err != nil {
		event.Err(err).Msg("SQL Execution Failed")
	} else {
		event.Msg("SQL Executed")
	}
}

// convertGormLevel maps GORM log levels to zerolog levels
func convertGormLevel(level logger.LogLevel) zerolog.Level {
	switch level {
	case logger.Silent:
		return zerolog.Disabled
	case logger.Error:
		return zerolog.ErrorLevel
	case logger.Warn:
		return zerolog.WarnLevel
	case logger.Info:
		return zerolog.InfoLevel
	default:
		return zerolog.InfoLevel
	}
}
