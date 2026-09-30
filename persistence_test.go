package sensor

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func waitResult(t *testing.T, e *Engine) VoteResult {
	t.Helper()
	select {
	case r := <-e.Results():
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting result")
		return VoteResult{}
	}
}

func submitFrame(t *testing.T, e *Engine, chs []ChannelID, seq int64, vals map[ChannelID]float64) {
	t.Helper()
	for _, ch := range chs {
		if err := e.Submit(Sample{Channel: ch, Seq: seq, Value: vals[ch]}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRestartKeepsFaultyChannelIsolated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)

	// 第一段运行：c 持续漂移，达到 FaultAfter 后被隔离并落盘。
	e1, err := NewEngine(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	for seq := int64(0); seq < 6; seq++ {
		submitFrame(t, e1, chs, seq, map[ChannelID]float64{"a": 10, "b": 10, "c": 14})
		waitResult(t, e1)
	}
	if st := e1.Snapshot().Statuses["c"]; st.State != StateFaulty {
		t.Fatalf("before restart c = %s, want faulty", st.State)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("state file not persisted: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	// 模拟崩溃后重启：即使 c 本拍读数恢复，也必须先处于隔离状态，
	// 按 RecoverAfter 窗口逐拍证明，而不是被立即重新纳入。
	e2, err := NewEngine(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	if st := e2.Snapshot().Statuses["c"]; st.State != StateFaulty {
		t.Fatalf("after restart c = %s, want still faulty", st.State)
	}

	// 重启后第一拍 c 给正常读数：仍是 faulty -> recovering，尚未参与表决。
	submitFrame(t, e2, chs, 6, map[ChannelID]float64{"a": 10, "b": 10, "c": 10})
	r := waitResult(t, e2)
	if r.Status == StatusFailed {
		t.Fatalf("post-restart tick failed: %s", r.Reason)
	}
	if st := e2.Snapshot().Statuses["c"]; st.State != StateRecovering {
		t.Fatalf("after one good tick c = %s, want recovering", st.State)
	}
	for _, ch := range r.Contributors {
		if ch == "c" {
			t.Fatal("recovering channel c contributed to vote before window elapsed")
		}
	}

	// 再两拍恢复窗口届满，c 重新纳入。
	submitFrame(t, e2, chs, 7, map[ChannelID]float64{"a": 10, "b": 10, "c": 10})
	waitResult(t, e2)
	submitFrame(t, e2, chs, 8, map[ChannelID]float64{"a": 10, "b": 10, "c": 10})
	waitResult(t, e2)
	if st := e2.Snapshot().Statuses["c"]; st.State != StateHealthy {
		t.Fatalf("after recovery window c = %s, want healthy", st.State)
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRestartResumesSeqAndHistory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)

	e1, err := NewEngine(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	for seq := int64(0); seq < 3; seq++ {
		submitFrame(t, e1, chs, seq, map[ChannelID]float64{"a": 10, "b": 10, "c": 10})
		waitResult(t, e1)
	}
	_ = e1.Close()

	e2, err := NewEngine(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	if view := e2.Snapshot(); view.Seq != 2 {
		t.Fatalf("restarted seq = %d, want 2", view.Seq)
	}
	e2.mu.Lock()
	histLen := len(e2.pipe.hist.get("a"))
	e2.mu.Unlock()
	if histLen == 0 {
		t.Fatal("recent history was lost across restart")
	}
}

func TestRestartRejectsMismatchedChannels(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	e1, err := NewEngine(DefaultConfig([]ChannelID{"a", "b", "c"}), path)
	if err != nil {
		t.Fatal(err)
	}
	submitFrame(t, e1, []ChannelID{"a", "b", "c"}, 0, map[ChannelID]float64{"a": 1, "b": 1, "c": 1})
	waitResult(t, e1)
	_ = e1.Close()

	if _, err := NewEngine(DefaultConfig([]ChannelID{"a", "b", "x"}), path); err == nil {
		t.Fatal("expected error when persisted channel set differs")
	}
}
