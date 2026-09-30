package redundancy

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFileStoreMissingAndCorrupt(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(filepath.Join(dir, "state.json"))
	if _, err := store.Load(); !errors.Is(err, ErrNoState) {
		t.Fatalf("Load on missing file = %v, want ErrNoState", err)
	}
	if err := os.WriteFile(store.path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); err == nil || errors.Is(err, ErrNoState) {
		t.Fatalf("Load on corrupt file = %v, want decode error", err)
	}
}

// isolateCh2 drives a drifting channel 2 into isolation.
func isolateCh2(m *Monitor) {
	feedRound(m, 0, 10, 10.1, 10.2)
	feedRound(m, 1, 10, 10.1, 11.5)
	feedRound(m, 2, 10, 10.1, 12.5)
	feedRound(m, 3, 10, 10.1, 13.5)
	feedRound(m, 4, 10, 10.1, 14.5)
}

func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(filepath.Join(dir, "state.json"))
	cfg := testConfig()
	before := newTestMonitor(t, cfg)
	before.store = store
	isolateCh2(before)
	snapBefore := before.Snapshot()
	if snapBefore.Channels[2].Health != HealthFaulty {
		t.Fatalf("pre-crash: ch2 health = %v, want faulty", snapBefore.Channels[2].Health)
	}

	// Simulate a crash: no graceful Close, just build a new monitor on
	// the same store. State was persisted after every round.
	after := newTestMonitor(t, cfg)
	after.store = store
	restored, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	after.restore(restored)

	if got := after.channels[2].health; got != HealthFaulty {
		t.Fatalf("post-restart: ch2 health = %v, want faulty (isolation must survive restart)", got)
	}
	if got := after.channels[2].mismatchRun; got != snapBefore.Channels[2].MismatchRun {
		t.Fatalf("post-restart: ch2 mismatch run = %d, want %d", got, snapBefore.Channels[2].MismatchRun)
	}
	if !after.hasLastValid || !approx(after.lastValid, snapBefore.Vote.Value) {
		t.Fatalf("post-restart: lastValid = %v/%v, want %v",
			after.lastValid, after.hasLastValid, snapBefore.Vote.Value)
	}

	// Voting must continue seamlessly: the faulty channel stays excluded
	// and the sequence continues where the crashed instance stopped.
	snap := feedRound(after, 5, 10, 10.1, 15.5)
	if snap.Seq != 5 {
		t.Fatalf("post-restart: seq = %d, want 5", snap.Seq)
	}
	if want := []ChannelID{0, 1}; !reflect.DeepEqual(contributors(snap), want) {
		t.Fatalf("post-restart: contributors = %v, want %v", contributors(snap), want)
	}

	// Recovery after restart follows the same window rules.
	snap = feedRound(after, 6, 10, 10.1, 10.15)
	if snap.Channels[2].Health != HealthRecovering {
		t.Fatalf("post-restart: ch2 health = %v, want recovering", snap.Channels[2].Health)
	}
	snap = feedRound(after, 7, 10, 10.1, 10.15)
	if snap.Channels[2].Health != HealthHealthy {
		t.Fatalf("post-restart: ch2 health = %v, want healthy", snap.Channels[2].Health)
	}
}

func TestPersistedHistoryBounded(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(filepath.Join(dir, "state.json"))
	cfg := testConfig()
	cfg.HistorySize = 4
	m := newTestMonitor(t, cfg)
	m.store = store
	for seq := uint64(0); seq < 10; seq++ {
		feedRound(m, seq, 10, 10.1, 10.2)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, pc := range st.Channels {
		if len(pc.History) > cfg.HistorySize {
			t.Fatalf("channel %d persisted history = %d, exceeds bound %d",
				pc.ID, len(pc.History), cfg.HistorySize)
		}
		if len(pc.History) != cfg.HistorySize {
			t.Fatalf("channel %d persisted history = %d, want %d",
				pc.ID, len(pc.History), cfg.HistorySize)
		}
	}
	if st.NextSeq != 10 {
		t.Fatalf("persisted NextSeq = %d, want 10", st.NextSeq)
	}
}

func TestRestartDoesNotReincludeFaulty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	cfg := testConfig()
	m := newTestMonitor(t, cfg)
	m.store = NewFileStore(path)
	isolateCh2(m)

	// Restart through the public constructor, as production code would.
	restarted, err := newMonitor(cfg, NewFileStore(path))
	if err != nil {
		t.Fatalf("newMonitor with store: %v", err)
	}
	snap := feedRound(restarted, 5, 10, 10.1, 10.15)
	if snap.Channels[2].Health == HealthHealthy {
		t.Fatal("restarted monitor wrongly re-included the faulty channel")
	}
	if want := []ChannelID{0, 1}; !reflect.DeepEqual(contributors(snap), want) {
		t.Fatalf("contributors = %v, want %v", contributors(snap), want)
	}
}
