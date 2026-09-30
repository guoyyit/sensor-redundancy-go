package redundancy

import (
	"sync"
	"testing"
	"time"
)

// waitForSeq polls until the monitor completes the round with the given
// sequence number or the deadline passes.
func waitForSeq(t *testing.T, m *Monitor, seq uint64) Snapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap := m.Snapshot()
		if snap.Seq >= seq && snap.Vote.Valid {
			return snap
		}
		time.Sleep(time.Millisecond)
	}
	snap := m.Snapshot()
	t.Fatalf("timed out waiting for round %d, last snapshot seq %d", seq, snap.Seq)
	return Snapshot{}
}

func TestConcurrentPipesConsistentReads(t *testing.T) {
	cfg := testConfig()
	m, err := NewMonitor(cfg, nil)
	if err != nil {
		t.Fatalf("NewMonitor: %v", err)
	}
	const rounds = 200
	var wg sync.WaitGroup
	for ch := 0; ch < cfg.Redundancy; ch++ {
		pipe := m.OpenPipe(ChannelID(ch))
		if pipe == nil {
			t.Fatalf("OpenPipe(%d) = nil", ch)
		}
		wg.Add(1)
		go func(base float64) {
			defer wg.Done()
			for seq := uint64(0); seq < rounds; seq++ {
				pipe.Write(Sample{Seq: seq, Value: 10 + base})
			}
		}(float64(ch) * 0.1)
	}

	// Concurrent reader: every snapshot must be self-consistent, i.e.
	// describe exactly one round across all channels.
	stop := make(chan struct{})
	var readerWg sync.WaitGroup
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			snap := m.Snapshot()
			if !snap.Vote.Valid {
				continue
			}
			for _, cs := range snap.Channels {
				if cs.Last.Seq != snap.Seq {
					t.Errorf("snapshot mixes rounds: snap seq %d, channel %d last seq %d",
						snap.Seq, cs.ID, cs.Last.Seq)
					return
				}
			}
			if len(snap.Vote.Contributors) == 0 {
				t.Error("valid vote without contributors")
				return
			}
		}
	}()

	wg.Wait()
	snap := waitForSeq(t, m, rounds-1)
	close(stop)
	readerWg.Wait()

	if snap.Vote.Degraded {
		t.Fatalf("all channels healthy, vote %+v should not be degraded", snap.Vote)
	}
	if len(snap.Vote.Contributors) != cfg.Redundancy {
		t.Fatalf("contributors = %v, want all %d channels", snap.Vote.Contributors, cfg.Redundancy)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConcurrentFaultIsolation(t *testing.T) {
	cfg := testConfig()
	m, err := NewMonitor(cfg, nil)
	if err != nil {
		t.Fatalf("NewMonitor: %v", err)
	}
	defer m.Close()
	const rounds = 50
	var wg sync.WaitGroup
	for ch := 0; ch < cfg.Redundancy; ch++ {
		pipe := m.OpenPipe(ChannelID(ch))
		wg.Add(1)
		go func(ch int) {
			defer wg.Done()
			for seq := uint64(0); seq < rounds; seq++ {
				v := 10.0
				if ch == 2 && seq >= 10 {
					v = 100.0 // channel 2 jumps away mid-stream
				}
				pipe.Write(Sample{Seq: seq, Value: v})
			}
		}(ch)
	}
	wg.Wait()
	snap := waitForSeq(t, m, rounds-1)

	if got := snap.Channels[2].Health; got != HealthFaulty {
		t.Fatalf("ch2 health = %v, want faulty", got)
	}
	if !approx(snap.Vote.Value, 10) {
		t.Fatalf("vote %v dragged by faulty channel", snap.Vote.Value)
	}
	for _, id := range snap.Vote.Contributors {
		if id == 2 {
			t.Fatalf("contributors %v include isolated channel 2", snap.Vote.Contributors)
		}
	}
}
