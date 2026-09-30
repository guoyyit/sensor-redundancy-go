package sensor

import "testing"

func TestUpdateConfigPreservesHealthProgress(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	e, err := NewEngine(DefaultConfig(chs), "")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// c 累计 2 次失配 -> suspected（计数保留）。
	submitFrame(t, e, chs, 0, map[ChannelID]float64{"a": 10, "b": 10, "c": 14})
	waitResult(t, e)
	submitFrame(t, e, chs, 1, map[ChannelID]float64{"a": 10, "b": 10, "c": 14})
	waitResult(t, e)
	before := e.Snapshot().Statuses["c"]
	if before.State != StateSuspected {
		t.Fatalf("precondition c=%s", before.State)
	}

	// 在线放宽漂移阈值：当前失配读数在新阈值下可能变匹配，
	// 但状态与既有计数必须原样保留，不被重置；下一拍起按新阈值判定。
	cfg2 := DefaultConfig(chs)
	cfg2.DriftThreshold = 5
	if err := e.UpdateConfig(cfg2); err != nil {
		t.Fatal(err)
	}
	after := e.Snapshot().Statuses["c"]
	if after.State != StateSuspected || after.MismatchStreak != before.MismatchStreak {
		t.Fatalf("health progress reset by config update: %+v -> %+v", before, after)
	}

	// 下一拍 c=14 在新阈值 5 下与参考一致：疑似应被清除，回到 healthy。
	submitFrame(t, e, chs, 2, map[ChannelID]float64{"a": 10, "b": 10, "c": 14})
	r := waitResult(t, e)
	if r.Status == StatusFailed {
		t.Fatalf("tick after reconfigure failed: %s", r.Reason)
	}
	if st := e.Snapshot().Statuses["c"]; st.State != StateHealthy {
		t.Fatalf("c after match under new threshold = %s, want healthy", st.State)
	}
}

func TestUpdateConfigRejectsChannelSetChange(t *testing.T) {
	e, err := NewEngine(DefaultConfig([]ChannelID{"a", "b", "c"}), "")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if err := e.UpdateConfig(DefaultConfig([]ChannelID{"a", "b", "d"})); err == nil {
		t.Fatal("expected error when changing channel set at runtime")
	}
}

func TestConfigValidation(t *testing.T) {
	base := DefaultConfig([]ChannelID{"a", "b", "c"})

	bad := base
	bad.FaultAfter = 1
	bad.SuspectAfter = 3
	if err := bad.Validate(); err == nil {
		t.Fatal("expected FaultAfter < SuspectAfter error")
	}

	bad = base
	bad.HistorySize = 1
	if err := bad.Validate(); err == nil {
		t.Fatal("expected HistorySize validation error")
	}

	if err := DefaultConfig([]ChannelID{"a", "a"}).Validate(); err == nil {
		t.Fatal("expected duplicate channel error")
	}
}
