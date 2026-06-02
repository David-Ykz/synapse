package autoscaler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type NamespaceLag struct {
	Namespace string `json:"namespace"`
	Lag       int64  `json:"lag"`
}

type MetricsClient struct {
	endpoint   string
	httpClient *http.Client
}

func NewMetricsClient(brokerAddr string) *MetricsClient {
	return &MetricsClient{
		endpoint:   fmt.Sprintf("http://%s/metrics/lag", brokerAddr),
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

// FetchLag returns a map of namespace -> current lag.
func (m *MetricsClient) FetchLag() (map[string]int64, error) {
	resp, err := m.httpClient.Get(m.endpoint)
	if err != nil {
		return nil, fmt.Errorf("fetch lag: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("broker metrics returned %d", resp.StatusCode)
	}
	var snapshots []NamespaceLag
	if err := json.NewDecoder(resp.Body).Decode(&snapshots); err != nil {
		return nil, fmt.Errorf("decode lag response: %w", err)
	}
	out := make(map[string]int64, len(snapshots))
	for _, s := range snapshots {
		out[s.Namespace] = s.Lag
	}
	return out, nil
}
