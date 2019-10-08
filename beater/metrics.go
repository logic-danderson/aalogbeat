package beater

import "expvar"

// Metrics that can be retrieed through the expvar web interface. Metrics must be 
// enabled through configuration in order for the web service to be started.
var (
	publishedEvents = expvar.NewMap("published_events")
)

func initMetrics(namespace string) {
	publishedEvents.Add(namespace, 0)
}

func addPublished(namespace string, n int) {
	numEvents := int64(n)
	publishedEvents.Add("total", numEvents)
	publishedEvents.Add(namespace, numEvents)
}