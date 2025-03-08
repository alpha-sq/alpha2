package crawler

import (
	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/debug"
	"github.com/gocolly/colly/v2/extensions"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func NewCollector() *colly.Collector {
	c := colly.NewCollector(colly.Debugger(&LogDebugger{}))
	extensions.RandomUserAgent(c)
	extensions.Referer(c)
	return c
}

type LogDebugger struct {
}

// Init initializes the LogDebugger
func (l *LogDebugger) Init() error {
	return nil
}

// Event receives Collector events and prints them to STDERR
func (l *LogDebugger) Event(e *debug.Event) {

	var lg *zerolog.Event
	if e.Type == "error" {
		lg = log.Error()
	} else {
		lg = log.Info()
	}

	lg.Uint32("CollectorID", e.CollectorID).
		Uint32("RequestID", e.RequestID).
		Str("Type", e.Type).
		Str("URL", e.Values["url"]).
		Timestamp().
		Msg("Crawler Event")
}
