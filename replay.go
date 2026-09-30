package redundancy

// Replay feeds a recorded sample sequence through a fresh Monitor with the
// given configuration and returns the snapshot after every completed round.
// It is deterministic: the same inputs always produce the same outputs.
// Samples are processed synchronously in the given order, so replay never
// depends on goroutine scheduling.
func Replay(cfg Config, samples []Sample) []Snapshot {
	m, err := newMonitor(cfg, nil)
	if err != nil {
		return nil
	}
	var out []Snapshot
	m.onRound = func(s Snapshot) { out = append(out, s) }
	for _, s := range samples {
		m.enqueue(s)
		m.tryRounds()
	}
	return out
}
