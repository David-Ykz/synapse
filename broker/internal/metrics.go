package synapse

import "github.com/prometheus/client_golang/prometheus"

var lagDesc = prometheus.NewDesc(
	"synapse_broker_lag",
	"Number of undelivered-or-unresolved messages for a broker queue (write index minus read index)",
	[]string{"queue"},
	nil,
)

// lagCollector adapts Server.GetLagSnapshot to the prometheus.Collector interface, computing lag
// for every scrape instead of maintaining a separately-updated counter
type lagCollector struct {
	server *Server
}

// NewLagCollector wraps server so its lag snapshot can be registered with a prometheus.Registry
func NewLagCollector(server *Server) prometheus.Collector {
	return &lagCollector{server: server}
}

func (c *lagCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- lagDesc
}

func (c *lagCollector) Collect(ch chan<- prometheus.Metric) {
	for _, snapshot := range c.server.GetLagSnapshot() {
		ch <- prometheus.MustNewConstMetric(lagDesc, prometheus.GaugeValue, float64(snapshot.Lag), snapshot.Namespace)
	}
}
