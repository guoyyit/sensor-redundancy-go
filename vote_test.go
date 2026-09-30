package sensor

import (
	"math"
	"testing"
)

func frame3(a, b, c float64) map[ChannelID]float64 {
	return map[ChannelID]float64{"a": a, "b": b, "c": c}
}

func newTestVoter(t *testing.T, chs []ChannelID) *voter {
	t.Helper()
	return newVoter(DefaultConfig(chs))
}

func TestVoteTripleHealthyMedian(t *testing.T) {
	v := newTestVoter(t, []ChannelID{"a", "b", "c"})
	r := v.vote(1, frame3(10, 10.2, 10.4), 3)
	if r.Status != StatusOK {
		t.Fatalf("status = %s, want ok", r.Status)
	}
	if math.Abs(r.Value-10.2) > 1e-9 {
		t.Fatalf("value = %v, want median 10.2", r.Value)
	}
}

func TestVoteTripleSingleDriftNotPulled(t *testing.T) {
	v := newTestVoter(t, []ChannelID{"a", "b", "c"})
	// c 漂移到 14，a/b 在阈值内一致：输出应为 a/b 均值，不被 c 拖偏。
	r := v.vote(1, frame3(10, 10.3, 14), 3)
	if r.Status != StatusOK {
		t.Fatalf("status = %s, want ok", r.Status)
	}
	if math.Abs(r.Value-10.15) > 1e-9 {
		t.Fatalf("value = %v, want 10.15 (agreed pair mean)", r.Value)
	}
	if len(r.Contributors) != 2 || r.Contributors[0] != "a" || r.Contributors[1] != "b" {
		t.Fatalf("contributors = %v, want [a b]", r.Contributors)
	}
}

func TestVoteTripleNoAgreementFails(t *testing.T) {
	v := newTestVoter(t, []ChannelID{"a", "b", "c"})
	r := v.vote(1, frame3(0, 5, 10), 3)
	if r.Status != StatusFailed || !math.IsNaN(r.Value) {
		t.Fatalf("got (%v,%s), want failed/NaN", r.Value, r.Status)
	}
}

func TestVoteDualAgreeAndDisagree(t *testing.T) {
	v := newTestVoter(t, []ChannelID{"a", "b"})

	ok := v.vote(1, map[ChannelID]float64{"a": 10, "b": 10.5}, 2)
	if ok.Status != StatusOK || ok.Value != 10.25 {
		t.Fatalf("dual agree got (%v,%s)", ok.Value, ok.Status)
	}

	bad := v.vote(2, map[ChannelID]float64{"a": 10, "b": 20}, 2)
	if bad.Status != StatusFailed || !math.IsNaN(bad.Value) {
		t.Fatalf("dual disagree got (%v,%s), want failed/NaN", bad.Value, bad.Status)
	}
}

func TestVoteDualDegradedSingle(t *testing.T) {
	// 双余度只剩一路且 MinHealthy=2（默认）：失效而非输出单路数值。
	v := newTestVoter(t, []ChannelID{"a", "b"})
	r := v.vote(1, map[ChannelID]float64{"a": 10}, 2)
	if r.Status != StatusFailed || !math.IsNaN(r.Value) {
		t.Fatalf("single in dual config got (%v,%s), want failed/NaN", r.Value, r.Status)
	}
}

func TestVoteQuad2v2Deterministic(t *testing.T) {
	chs := []ChannelID{"a", "b", "c", "d"}
	v := newTestVoter(t, chs)
	// 2:2 分裂：{a,b} 与 {c,d} 各自一致。平局应由配置顺序确定地选 {a,b}。
	r := v.vote(1, map[ChannelID]float64{"a": 10, "b": 10.2, "c": 20, "d": 20.2}, 4)
	if r.Status != StatusOK {
		t.Fatalf("status = %s, want ok", r.Status)
	}
	if math.Abs(r.Value-10.1) > 1e-9 {
		t.Fatalf("value = %v, want 10.1 (first-in-config cluster)", r.Value)
	}
	if len(r.Contributors) != 2 {
		t.Fatalf("contributors = %v, want a,b cluster", r.Contributors)
	}
}

func TestVoteQuad3v1Majority(t *testing.T) {
	chs := []ChannelID{"a", "b", "c", "d"}
	v := newTestVoter(t, chs)
	r := v.vote(1, map[ChannelID]float64{"a": 10, "b": 10.1, "c": 9.95, "d": 30}, 4)
	if r.Status != StatusOK {
		t.Fatalf("status = %s, want ok", r.Status)
	}
	if math.Abs(r.Value-10.0166666667) > 1e-6 {
		t.Fatalf("value = %v, want majority trio mean", r.Value)
	}
	if len(r.Contributors) != 3 {
		t.Fatalf("contributors = %v, want the 3 healthy channels", r.Contributors)
	}
}

func TestVoteDegradedThreeWithTwoAvailable(t *testing.T) {
	v := newTestVoter(t, []ChannelID{"a", "b", "c"})
	r := v.vote(1, map[ChannelID]float64{"a": 10, "b": 10.4}, 3)
	if r.Status != StatusDegraded {
		t.Fatalf("status = %s, want degraded", r.Status)
	}
	if math.Abs(r.Value-10.2) > 1e-9 {
		t.Fatalf("value = %v, want pair mean 10.2", r.Value)
	}
}

func TestVoteZeroAvailableFails(t *testing.T) {
	v := newTestVoter(t, []ChannelID{"a", "b", "c"})
	r := v.vote(1, map[ChannelID]float64{}, 3)
	if r.Status != StatusFailed || !math.IsNaN(r.Value) {
		t.Fatalf("empty frame got (%v,%s), want failed/NaN", r.Value, r.Status)
	}
}
