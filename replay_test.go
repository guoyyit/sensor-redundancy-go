package redundancy

import (
	"reflect"
	"testing"
)

// recordedSequence covers drift, isolation, recovery and a jitter round.
func recordedSequence() []Sample {
	rounds := [][]float64{
		{10, 10.1, 10.2},
		{10, 10.1, 11.5},
		{10, 10.1, 12.5},
		{10, 10.1, 13.5},
		{10, 10.1, 14.5},
		{10, 10.1, 10.15},
		{10, 10.1, 10.15},
		{10, 10.1, 10.15},
		{10, 50, 10.15},
		{10, 10.1, 10.15},
	}
	var out []Sample
	for seq, values := range rounds {
		for ch, v := range values {
			out = append(out, Sample{Channel: ChannelID(ch), Seq: uint64(seq), Value: v})
		}
	}
	return out
}

func TestReplayDeterministic(t *testing.T) {
	cfg := testConfig()
	samples := recordedSequence()
	first := Replay(cfg, samples)
	second := Replay(cfg, samples)
	if len(first) != len(samples)/cfg.Redundancy {
		t.Fatalf("replay produced %d snapshots, want %d", len(first), len(samples)/cfg.Redundancy)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("two replays of the same sequence diverged")
	}
}

func TestReplayReproducesIsolation(t *testing.T) {
	cfg := testConfig()
	snaps := Replay(cfg, recordedSequence())
	wantHealth := []HealthState{
		HealthHealthy,    // round 0: all agree
		HealthSuspect,    // round 1: drift starts
		HealthSuspect,    // round 2
		HealthFaulty,     // round 3: isolated
		HealthFaulty,     // round 4
		HealthRecovering, // round 5: back to normal
		HealthHealthy,    // round 6: recovery window complete
		HealthHealthy,    // round 7
		HealthHealthy,    // round 8: ch1 glitches, ch2 unaffected
		HealthHealthy,    // round 9
	}
	for i, want := range wantHealth {
		if got := snaps[i].Channels[2].Health; got != want {
			t.Fatalf("round %d: ch2 health = %v, want %v", i, got, want)
		}
	}
	if got := snaps[8].Channels[1].Health; got != HealthSuspect {
		t.Fatalf("round 8: ch1 health = %v, want suspect after glitch", got)
	}
	if got := snaps[9].Channels[1].Health; got != HealthHealthy {
		t.Fatalf("round 9: ch1 health = %v, want healthy", got)
	}
}

func TestReplayMatchesLiveMonitor(t *testing.T) {
	cfg := testConfig()
	samples := recordedSequence()
	replayed := Replay(cfg, samples)

	live := newTestMonitor(t, cfg)
	var liveSnaps []Snapshot
	live.onRound = func(s Snapshot) { liveSnaps = append(liveSnaps, s) }
	for _, s := range samples {
		live.enqueue(s)
		live.tryRounds()
	}
	if !reflect.DeepEqual(replayed, liveSnaps) {
		t.Fatal("replay output differs from live monitor output")
	}
}

func TestReplayArrivalOrderIndependent(t *testing.T) {
	cfg := testConfig()
	samples := recordedSequence()
	// Reverse the arrival order within each round; per-channel order and
	// sequence numbers are unchanged, so the outcome must be identical.
	var shuffled []Sample
	for seq := 0; seq*cfg.Redundancy < len(samples); seq++ {
		for ch := cfg.Redundancy - 1; ch >= 0; ch-- {
			shuffled = append(shuffled, samples[seq*cfg.Redundancy+ch])
		}
	}
	if !reflect.DeepEqual(Replay(cfg, samples), Replay(cfg, shuffled)) {
		t.Fatal("arrival order within a round changed the outcome")
	}
}
