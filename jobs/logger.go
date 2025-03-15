package jobs

import "github.com/rs/zerolog/log"

type JobZeroLogger struct{}

func (l *JobZeroLogger) Trace(msg string, args ...any) {
	log.Trace().Msgf(msg, args...)
}

func (l *JobZeroLogger) Debug(msg string, args ...any) {
	log.Debug().Msgf(msg, args...)
}

func (l *JobZeroLogger) Info(msg string, args ...any) {
	log.Info().Msgf(msg, args...)
}

func (l *JobZeroLogger) Warn(msg string, args ...any) {
	log.Warn().Msgf(msg, args...)
}

func (l *JobZeroLogger) Error(msg string, args ...any) {
	log.Error().Msgf(msg, args...)
}
