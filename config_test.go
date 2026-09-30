package redundancy

import "testing"

func floatPtr(v float64) *float64 { return &v }

func intPtr(v int) *int { return &v }

func TestThresholdUpdateOnline(t *testing.T) {
	m := newTestMonitor(t, testConfig())

	snap := feedRound(m, 0, 10, 10.1, 10.9)
	if snap.Channels[2].Health != HealthHealthy {
		t.Fatalf("round 0: ch2 health = %v, want healthy under threshold 1.0", snap.Channels[2].Health)
	}

	// Tighten the threshold online; the next round is evaluated with the
	// new threshold, the previous rounds are unaffected.
	m.applyConfig(ConfigUpdate{Threshold: floatPtr(0.4)})
	snap = feedRound(m, 1, 10, 10.1, 10.9)
	if snap.Channels[2].Health != HealthSuspect {
		t.Fatalf("round 1: ch2 health = %v, want suspect under threshold 0.4", snap.Channels[2].Health)
	}
	if !approx(snap.Vote.Value, 10.05) {
		t.Fatalf("round 1: vote %v, want 10.05 (ch2 outside tightened threshold)", snap.Vote.Value)
	}

	// Invalid updates are ignored.
	m.applyConfig(ConfigUpdate{Threshold: floatPtr(-1)})
	snap = feedRound(m, 2, 10, 10.1, 10.9)
	if snap.Channels[2].MismatchRun != 2 {
		t.Fatalf("round 2: ch2 mismatch run = %d, want 2 (threshold still 0.4)",
			snap.Channels[2].MismatchRun)
	}
}

func TestRecoverWindowUpdateOnline(t *testing.T) {
	m := newTestMonitor(t, testConfig())
	isolateCh2(m)

	snap := feedRound(m, 5, 10, 10.1, 10.15)
	if snap.Channels[2].Health != HealthRecovering {
		t.Fatalf("round 5: ch2 health = %v, want recovering", snap.Channels[2].Health)
	}

	// Widen the recovery window mid-recovery: the in-progress match run
	// is preserved, only the target moves.
	m.applyConfig(ConfigUpdate{RecoverWindow: intPtr(3)})
	snap = feedRound(m, 6, 10, 10.1, 10.15)
	if snap.Channels[2].Health != HealthRecovering {
		t.Fatalf("round 6: ch2 health = %v, want still recovering under widened window",
			snap.Channels[2].Health)
	}
	if snap.Channels[2].MatchRun != 2 {
		t.Fatalf("round 6: ch2 match run = %d, want 2 (run preserved across update)",
			snap.Channels[2].MatchRun)
	}
	snap = feedRound(m, 7, 10, 10.1, 10.15)
	if snap.Channels[2].Health != HealthHealthy {
		t.Fatalf("round 7: ch2 health = %v, want healthy after 3 matches", snap.Channels[2].Health)
	}
}

func TestUpdateConfigThroughPublicAPI(t *testing.T) {
	cfg := testConfig()
	m, err := NewMonitor(cfg, nil)
	if err != nil {
		t.Fatalf("NewMonitor: %v", err)
	}
	defer m.Close()

	pipes := make([]*Pipe, cfg.Redundancy)
	for ch := range pipes {
		pipes[ch] = m.OpenPipe(ChannelID(ch))
	}
	feed := func(seq uint64, values ...float64) {
		for ch, v := range values {
			pipes[ch].Write(Sample{Seq: seq, Value: v})
		}
	}

	feed(0, 10, 10.1, 10.9)
	waitForSeq(t, m, 0)

	m.UpdateConfig(ConfigUpdate{Threshold: floatPtr(0.4)})
	m.UpdateConfig(ConfigUpdate{RecoverWindow: intPtr(1)})

	for seq := uint64(1); seq <= 3; seq++ {
		feed(seq, 10, 10.1, 10.9)
	}
	snap := waitForSeq(t, m, 3)
	if got := snap.Channels[2].Health; got != HealthFaulty {
		t.Fatalf("ch2 health = %v, want faulty after threshold tightened online", got)
	}
}
