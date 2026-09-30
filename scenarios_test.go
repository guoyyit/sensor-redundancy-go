package sensor

import (
	"math"
	"testing"
)

// runPipeline 以确定性方式推进若干拍并返回最终 pipeline。
func runPipeline(t *testing.T, cfg Config, seqs []map[ChannelID]float64) *pipeline {
	t.Helper()
	chs := cfg.Channels
	p := newPipeline(cfg)
	for seq, vals := range seqs {
		frame := map[ChannelID]Sample{}
		for _, ch := range chs {
			frame[ch] = Sample{Channel: ch, Seq: int64(seq), Value: vals[ch]}
		}
		_, _ = p.tick(int64(seq), frame)
	}
	return p
}

func TestDualRedundancyFaultThenFail(t *testing.T) {
	chs := []ChannelID{"a", "b"}
	cfg := DefaultConfig(chs)
	// 双余度：两通道一致时输出；b 漂移后两者分歧，双余度无法仲裁 -> 明确失效。
	seqs := []map[ChannelID]float64{
		{"a": 10, "b": 10},
		{"a": 10, "b": 10},
		{"a": 10, "b": 20},
	}
	results, err := Replay(cfg, sequence(chs, seqs))
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != StatusOK {
		t.Fatalf("healthy dual result = %s", results[0].Status)
	}
	for i := 2; i < len(results); i++ {
		r := results[i]
		// 双余度无第三方仲裁：分歧只能明确报失效，且不会输出错误数值。
		if r.Status != StatusFailed || !math.IsNaN(r.Value) {
			t.Fatalf("disagreeing dual seq %d got (%v,%s), want failed/NaN", i, r.Value, r.Status)
		}
	}
}

func TestStuckChannelIsolated(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)
	// 真实物理量从 10 逐渐变化到 12，c 卡死在 5 不动。
	seqs := []map[ChannelID]float64{
		{"a": 10.0, "b": 10.0, "c": 5},
		{"a": 10.4, "b": 10.4, "c": 5},
		{"a": 10.8, "b": 10.8, "c": 5},
		{"a": 11.2, "b": 11.2, "c": 5},
		{"a": 11.6, "b": 11.6, "c": 5},
		{"a": 12.0, "b": 12.0, "c": 5},
		{"a": 12.4, "b": 12.4, "c": 5},
		{"a": 12.8, "b": 12.8, "c": 5},
	}
	p := runPipeline(t, cfg, seqs)
	if st := p.statuses["c"]; st.State != StateFaulty {
		t.Fatalf("stuck c = %s, want faulty", st.State)
	}

	results, err := Replay(cfg, sequence(chs, seqs))
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range results {
		if r.Status == StatusFailed {
			t.Fatalf("seq %d failed while a/b healthy", i)
		}
		// 输出应贴近 a/b，绝不被卡在 5 的通道拖偏。
		if math.Abs(r.Value-seqs[i]["a"]) > 0.2 {
			t.Fatalf("seq %d output %v pulled toward stuck channel", i, r.Value)
		}
	}
}

func TestJumpChannelIsolated(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs) // JumpThreshold=5
	// c 反复大幅跳变；a/b 稳定。c 因持续失配被隔离。
	seqs := []map[ChannelID]float64{
		{"a": 10, "b": 10, "c": 20},
		{"a": 10, "b": 10, "c": 0},
		{"a": 10, "b": 10, "c": 30},
		{"a": 10, "b": 10, "c": 0},
	}
	p := runPipeline(t, cfg, seqs)
	if st := p.statuses["c"]; st.State != StateFaulty {
		t.Fatalf("jumping c = %s, want faulty", st.State)
	}

	results, _ := Replay(cfg, sequence(chs, seqs))
	for i, r := range results {
		if r.Status == StatusFailed {
			t.Fatalf("seq %d failed while a/b healthy", i)
		}
		if math.Abs(r.Value-10) > 0.2 {
			t.Fatalf("seq %d output %v pulled by jumping channel", i, r.Value)
		}
	}
}

func TestSingleChannelAllowedWhenConfigured(t *testing.T) {
	// MinHealthy=1 时，三路退到单路仍允许 degraded 输出真实数值。
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)
	cfg.MinHealthy = 1

	p := newPipeline(cfg)
	frame := func(seq int64, vals map[ChannelID]float64) VoteResult {
		f := map[ChannelID]Sample{}
		for _, ch := range chs {
			f[ch] = Sample{Channel: ch, Seq: seq, Value: vals[ch]}
		}
		r, _ := p.tick(seq, f)
		return r
	}

	// 先隔离 c（a/b 一致），再隔离 b：最终只剩 a 一路仍可降级输出。
	var last VoteResult
	for seq := 0; seq < 8; seq++ {
		vals := map[ChannelID]float64{"a": 10, "b": 10, "c": 30}
		if seq >= 4 {
			// c 隔离后，让 b 也开始偏离；此时仅 a 可用。
			vals["b"] = -20
		}
		last = frame(int64(seq), vals)
	}
	if last.Status != StatusDegraded || math.Abs(last.Value-10) > 1e-9 {
		t.Fatalf("single-survivor result = (%v,%s), want degraded 10", last.Value, last.Status)
	}
	if len(last.Contributors) != 1 || last.Contributors[0] != "a" {
		t.Fatalf("contributors = %v, want only a", last.Contributors)
	}
}
