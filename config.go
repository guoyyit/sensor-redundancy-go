package redundancy

// ConfigUpdate carries online-adjustable configuration fields. Nil fields
// are left unchanged.
type ConfigUpdate struct {
	Threshold     *float64
	RecoverWindow *int
}

// UpdateConfig applies a configuration change at the next round boundary.
// Because the change is applied by the ingest loop between rounds, it can
// never tear an ongoing health evaluation: every round is evaluated with
// one consistent configuration. Invalid values (non-positive threshold,
// recovery window < 1) are ignored.
func (m *Monitor) UpdateConfig(u ConfigUpdate) {
	select {
	case m.configCh <- u:
	case <-m.quit:
	}
}
