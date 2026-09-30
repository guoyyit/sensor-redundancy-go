package redundancy

import (
	"reflect"
	"testing"
)

func testConfig() Config {
	return Config{
		Redundancy:    3,
		Threshold:     1.0,
		MismatchLimit: 3,
		RecoverWindow: 2,
		HistorySize:   8,
	}
}

func newTestMonitor(t *testing.T, cfg Config) *Monitor {
	t.Helper()
	m, err := newMonitor(cfg, nil)
	if err != nil {
		t.Fatalf("newMonitor: %v", err)
	}
	return m
}

func feedRound(m *Monitor, seq uint64, values ...float64) Snapshot {
	for i, v := range values {
		m.enqueue(Sample{Channel: ChannelID(i), Seq: seq, Value: v})
	}
	m.tryRounds()
	return m.Snapshot()
}

func approx(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d < 1e-9
}

func contributors(snap Snapshot) []ChannelID {
	return snap.Vote.Contributors
}

func TestInvalidConfig(t *testing.T) {
	if _, err := NewMonitor(Config{Redundancy: 5, Threshold: 1}, nil); err == nil {
		t.Fatal("expected error for redundancy 5")
	}
	if _, err := NewMonitor(Config{Redundancy: 3, Threshold: 0}, nil); err == nil {
		t.Fatal("expected error for zero threshold")
	}
}

func TestDriftIsolation(t *testing.T) {
	m := newTestMonitor(t, testConfig())

	snap := feedRound(m, 0, 10, 10.1, 10.2)
	if !snap.Vote.Valid || snap.Vote.Degraded {
		t.Fatalf("round 0: expected valid full vote, got %+v", snap.Vote)
	}

	snap = feedRound(m, 1, 10, 10.1, 11.5)
	if snap.Channels[2].Health != HealthSuspect {
		t.Fatalf("round 1: ch2 health = %v, want suspect", snap.Channels[2].Health)
	}
	if !approx(snap.Vote.Value, 10.05) {
		t.Fatalf("round 1: vote %v dragged by drifter", snap.Vote.Value)
	}

	feedRound(m, 2, 10, 10.1, 12.5)
	snap = feedRound(m, 3, 10, 10.1, 13.5)
	if snap.Channels[2].Health != HealthFaulty {
		t.Fatalf("round 3: ch2 health = %v, want faulty", snap.Channels[2].Health)
	}

	snap = feedRound(m, 4, 10, 10.1, 14.5)
	if want := []ChannelID{0, 1}; !reflect.DeepEqual(contributors(snap), want) {
		t.Fatalf("round 4: contributors = %v, want %v", contributors(snap), want)
	}
	if !snap.Vote.Degraded || !approx(snap.Vote.Value, 10.05) {
		t.Fatalf("round 4: vote %+v, want degraded 10.05", snap.Vote)
	}
}

func TestJumpIsolation(t *testing.T) {
	m := newTestMonitor(t, testConfig())

	feedRound(m, 0, 10, 10.1, 10.2)
	snap := feedRound(m, 1, 10, 100, 10.2)
	if snap.Channels[1].Health != HealthSuspect {
		t.Fatalf("round 1: ch1 health = %v, want suspect", snap.Channels[1].Health)
	}
	if !approx(snap.Vote.Value, 10.1) {
		t.Fatalf("round 1: vote %v dragged by jumper", snap.Vote.Value)
	}

	feedRound(m, 2, 10, 100, 10.2)
	snap = feedRound(m, 3, 10, 100, 10.2)
	if snap.Channels[1].Health != HealthFaulty {
		t.Fatalf("round 3: ch1 health = %v, want faulty", snap.Channels[1].Health)
	}

	snap = feedRound(m, 4, 10, 100, 10.2)
	if want := []ChannelID{0, 2}; !reflect.DeepEqual(contributors(snap), want) {
		t.Fatalf("round 4: contributors = %v, want %v", contributors(snap), want)
	}
}

func TestStuckIsolation(t *testing.T) {
	m := newTestMonitor(t, testConfig())

	feedRound(m, 0, 10, 10, 10)
	feedRound(m, 1, 11, 11, 10) // stuck ch2 still within threshold
	snap := feedRound(m, 2, 12, 12, 10)
	if snap.Channels[2].Health != HealthSuspect {
		t.Fatalf("round 2: ch2 health = %v, want suspect", snap.Channels[2].Health)
	}
	feedRound(m, 3, 13, 13, 10)
	snap = feedRound(m, 4, 14, 14, 10)
	if snap.Channels[2].Health != HealthFaulty {
		t.Fatalf("round 4: ch2 health = %v, want faulty", snap.Channels[2].Health)
	}
	snap = feedRound(m, 5, 15, 15, 10)
	if !approx(snap.Vote.Value, 15) {
		t.Fatalf("round 5: vote %v, want 15 (stuck channel excluded)", snap.Vote.Value)
	}
}

func TestTransientJitterNotIsolated(t *testing.T) {
	m := newTestMonitor(t, testConfig())

	feedRound(m, 0, 10, 10.1, 10.2)
	snap := feedRound(m, 1, 10, 50, 10.2) // one-round glitch
	if snap.Channels[1].Health != HealthSuspect {
		t.Fatalf("round 1: ch1 health = %v, want suspect", snap.Channels[1].Health)
	}
	snap = feedRound(m, 2, 10, 10.1, 10.2) // glitch gone
	if snap.Channels[1].Health != HealthHealthy {
		t.Fatalf("round 2: ch1 health = %v, want healthy", snap.Channels[1].Health)
	}
	snap = feedRound(m, 3, 10, 90, 10.2) // another isolated glitch
	snap = feedRound(m, 4, 10, 10.1, 10.2)
	if snap.Channels[1].Health != HealthHealthy {
		t.Fatalf("round 4: ch1 health = %v, want healthy", snap.Channels[1].Health)
	}
	if want := []ChannelID{0, 1, 2}; !reflect.DeepEqual(contributors(snap), want) {
		t.Fatalf("round 4: contributors = %v, want %v", contributors(snap), want)
	}
}

func TestDegradationToSingleChannel(t *testing.T) {
	m := newTestMonitor(t, testConfig())

	feedRound(m, 0, 10, 10.1, 10.2)
	feedRound(m, 1, 10, 10.1, 11.5)
	feedRound(m, 2, 10, 10.1, 12.5)
	feedRound(m, 3, 10, 10.1, 13.5) // ch2 faulty

	// ch1 jumps away: the two remaining participants disagree, so the
	// output is invalid rather than arbitrary.
	snap := feedRound(m, 4, 10, 100, 14.5)
	if snap.Vote.Valid {
		t.Fatalf("round 4: vote %+v, want invalid during disagreement", snap.Vote)
	}
	feedRound(m, 5, 10, 100, 15.5)
	snap = feedRound(m, 6, 10, 100, 16.5)
	if snap.Channels[1].Health != HealthFaulty {
		t.Fatalf("round 6: ch1 health = %v, want faulty", snap.Channels[1].Health)
	}

	snap = feedRound(m, 7, 10, 100, 17.5)
	if !snap.Vote.Valid || !snap.Vote.Degraded {
		t.Fatalf("round 7: vote %+v, want valid degraded single-channel output", snap.Vote)
	}
	if !approx(snap.Vote.Value, 10) {
		t.Fatalf("round 7: vote %v, want 10 from the last healthy channel", snap.Vote.Value)
	}
	if want := []ChannelID{0}; !reflect.DeepEqual(contributors(snap), want) {
		t.Fatalf("round 7: contributors = %v, want %v", contributors(snap), want)
	}
}

func TestAllChannelsFailedInvalid(t *testing.T) {
	m := newTestMonitor(t, testConfig())

	feedRound(m, 0, 10, 10.1, 10.2)
	for seq := uint64(1); seq <= 3; seq++ {
		feedRound(m, seq, 50, 60, 70)
	}
	snap := feedRound(m, 4, 50, 60, 70)
	if snap.Vote.Valid {
		t.Fatalf("vote %+v must be invalid when no channel is trustworthy", snap.Vote)
	}
	for i, c := range snap.Channels {
		if c.Health != HealthFaulty {
			t.Fatalf("channel %d health = %v, want faulty", i, c.Health)
		}
	}
}

func TestRecoveryReinclusion(t *testing.T) {
	m := newTestMonitor(t, testConfig())

	feedRound(m, 0, 10, 10.1, 10.2)
	feedRound(m, 1, 10, 10.1, 11.5)
	feedRound(m, 2, 10, 10.1, 12.5)
	feedRound(m, 3, 10, 10.1, 13.5) // ch2 faulty

	snap := feedRound(m, 4, 10, 10.1, 10.15) // ch2 back to normal
	if snap.Channels[2].Health != HealthRecovering {
		t.Fatalf("round 4: ch2 health = %v, want recovering", snap.Channels[2].Health)
	}
	if want := []ChannelID{0, 1}; !reflect.DeepEqual(contributors(snap), want) {
		t.Fatalf("round 4: contributors = %v, want %v (not yet re-included)", contributors(snap), want)
	}

	snap = feedRound(m, 5, 10, 10.1, 10.15) // recovery window complete
	if snap.Channels[2].Health != HealthHealthy {
		t.Fatalf("round 5: ch2 health = %v, want healthy", snap.Channels[2].Health)
	}

	snap = feedRound(m, 6, 10, 10.1, 10.15)
	if want := []ChannelID{0, 1, 2}; !reflect.DeepEqual(contributors(snap), want) {
		t.Fatalf("round 6: contributors = %v, want %v (re-included)", contributors(snap), want)
	}
	if snap.Vote.Degraded {
		t.Fatalf("round 6: vote %+v, want full redundancy", snap.Vote)
	}
}

func TestRecoveryRelapse(t *testing.T) {
	m := newTestMonitor(t, testConfig())

	feedRound(m, 0, 10, 10.1, 10.2)
	feedRound(m, 1, 10, 10.1, 11.5)
	feedRound(m, 2, 10, 10.1, 12.5)
	feedRound(m, 3, 10, 10.1, 13.5) // ch2 faulty

	snap := feedRound(m, 4, 10, 10.1, 10.15)
	if snap.Channels[2].Health != HealthRecovering {
		t.Fatalf("round 4: ch2 health = %v, want recovering", snap.Channels[2].Health)
	}
	snap = feedRound(m, 5, 10, 10.1, 50) // relapse
	if snap.Channels[2].Health != HealthFaulty {
		t.Fatalf("round 5: ch2 health = %v, want faulty after relapse", snap.Channels[2].Health)
	}
	snap = feedRound(m, 6, 10, 10.1, 10.15)
	if snap.Channels[2].Health != HealthRecovering {
		t.Fatalf("round 6: ch2 health = %v, want recovering", snap.Channels[2].Health)
	}
	if snap.Channels[2].MatchRun != 1 {
		t.Fatalf("round 6: ch2 match run = %d, want 1 (window restarted)", snap.Channels[2].MatchRun)
	}
}

func TestDualRedundancy(t *testing.T) {
	cfg := testConfig()
	cfg.Redundancy = 2
	cfg.MismatchLimit = 2
	m := newTestMonitor(t, cfg)

	snap := feedRound(m, 0, 5, 5.2)
	if !snap.Vote.Valid || snap.Vote.Degraded || !approx(snap.Vote.Value, 5.1) {
		t.Fatalf("round 0: vote %+v, want valid 5.1", snap.Vote)
	}

	snap = feedRound(m, 1, 5, 9) // ch1 jumps: disagreement
	if snap.Vote.Valid {
		t.Fatalf("round 1: vote %+v, want invalid during disagreement", snap.Vote)
	}
	snap = feedRound(m, 2, 5, 9)
	if snap.Channels[1].Health != HealthFaulty {
		t.Fatalf("round 2: ch1 health = %v, want faulty", snap.Channels[1].Health)
	}

	snap = feedRound(m, 3, 5, 9)
	if !snap.Vote.Valid || !snap.Vote.Degraded || !approx(snap.Vote.Value, 5) {
		t.Fatalf("round 3: vote %+v, want degraded 5", snap.Vote)
	}
}

func TestQuadRedundancy(t *testing.T) {
	cfg := testConfig()
	cfg.Redundancy = 4
	m := newTestMonitor(t, cfg)

	feedRound(m, 0, 20, 20.1, 19.9, 20.2)
	feedRound(m, 1, 20, 20.1, 19.9, 25)
	feedRound(m, 2, 20, 20.1, 19.9, 26)
	snap := feedRound(m, 3, 20, 20.1, 19.9, 27)
	if snap.Channels[3].Health != HealthFaulty {
		t.Fatalf("round 3: ch3 health = %v, want faulty", snap.Channels[3].Health)
	}
	snap = feedRound(m, 4, 20, 20.1, 19.9, 28)
	if want := []ChannelID{0, 1, 2}; !reflect.DeepEqual(contributors(snap), want) {
		t.Fatalf("round 4: contributors = %v, want %v", contributors(snap), want)
	}
	if !snap.Vote.Degraded {
		t.Fatalf("round 4: vote %+v, want degraded", snap.Vote)
	}
}

func TestSnapshotSeqAndHistoryBound(t *testing.T) {
	m := newTestMonitor(t, testConfig())
	for seq := uint64(0); seq < 20; seq++ {
		feedRound(m, seq, 10, 10.1, 10.2)
	}
	snap := m.Snapshot()
	if snap.Seq != 19 {
		t.Fatalf("snapshot seq = %d, want 19", snap.Seq)
	}
	for i, c := range m.channels {
		if len(c.history) > m.cfg.HistorySize {
			t.Fatalf("channel %d history = %d, exceeds bound %d", i, len(c.history), m.cfg.HistorySize)
		}
	}
}

func TestUncompletableRoundSkipped(t *testing.T) {
	m := newTestMonitor(t, testConfig())

	// Channel 0 delivers round 0, but channels 1 and 2 start at round 1:
	// round 0 can never complete and must be skipped, not spun on.
	m.enqueue(Sample{Channel: 0, Seq: 0, Value: 10})
	for seq := uint64(1); seq <= 3; seq++ {
		m.enqueue(Sample{Channel: 0, Seq: seq, Value: 10})
	}
	for seq := uint64(1); seq <= 3; seq++ {
		m.enqueue(Sample{Channel: 1, Seq: seq, Value: 10.1})
		m.enqueue(Sample{Channel: 2, Seq: seq, Value: 10.2})
	}
	m.tryRounds()

	snap := m.Snapshot()
	if snap.Seq != 3 {
		t.Fatalf("snapshot seq = %d, want 3 (rounds 1-3 completed)", snap.Seq)
	}
	if !snap.Vote.Valid {
		t.Fatalf("vote %+v, want valid after skipping uncompletable round", snap.Vote)
	}
}
