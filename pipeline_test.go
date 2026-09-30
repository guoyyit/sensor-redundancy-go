package sensor

import (
	"math"
	"testing"
)

func mkSamples(chs []ChannelID, seq int64, vals map[ChannelID]float64) []Sample {
	out := make([]Sample, 0, len(chs))
	for _, ch := range chs {
		out = append(out, Sample{Channel: ch, Seq: seq, Value: vals[ch]})
	}
	return out
}

func sequence(chs []ChannelID, seqVals []map[ChannelID]float64) []Sample {
	var all []Sample
	for seq, vals := range seqVals {
		all = append(all, mkSamples(chs, int64(seq), vals)...)
	}
	return all
}

func TestReplayDriftChannelIsolatedAndOutputStable(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)

	// 前 2 拍全健康；之后 c 持续漂移到 14。FaultAfter=3，第 5 拍 c 被隔离。
	seqs := []map[ChannelID]float64{
		{"a": 10, "b": 10, "c": 10},
		{"a": 10, "b": 10, "c": 10},
		{"a": 10, "b": 10, "c": 14},
		{"a": 10, "b": 10, "c": 14},
		{"a": 10, "b": 10, "c": 14},
		{"a": 10, "b": 10, "c": 14},
	}
	results, err := Replay(cfg, sequence(chs, seqs))
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range results {
		if r.Status == StatusFailed {
			t.Fatalf("seq %d unexpectedly failed", i)
		}
		if math.Abs(r.Value-10) > 0.01 {
			t.Fatalf("seq %d output %v pulled by faulty channel", i, r.Value)
		}
	}

	// 验证 c 最终被隔离：再跑一次，直接检查内部状态。
	p := newPipeline(cfg)
	var statuses map[ChannelID]ChannelStatus
	for seq, vals := range seqs {
		frame := map[ChannelID]Sample{}
		for _, ch := range chs {
			frame[ch] = Sample{Channel: ch, Seq: int64(seq), Value: vals[ch]}
		}
		_, _ = p.tick(int64(seq), frame)
		statuses = p.statuses
	}
	if statuses["c"].State != StateFaulty {
		t.Fatalf("channel c state = %s, want faulty", statuses["c"].State)
	}
	if statuses["a"].State != StateHealthy || statuses["b"].State != StateHealthy {
		t.Fatalf("healthy channels changed: a=%s b=%s", statuses["a"].State, statuses["b"].State)
	}
}

func TestReplayDegradationThenAllFail(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)

	// c 先被隔离（a/b 仍健康 => degraded pair output），随后 a 也与 b 分歧 => 无一致对 => failed。
	seqs := []map[ChannelID]float64{
		{"a": 10, "b": 10, "c": 20},
		{"a": 10, "b": 10, "c": 20},
		{"a": 10, "b": 10, "c": 20},
		{"a": 10, "b": 10, "c": 20},
		{"a": 0, "b": 10, "c": 20},
	}
	results, err := Replay(cfg, sequence(chs, seqs))
	if err != nil {
		t.Fatal(err)
	}

	if results[3].Status != StatusDegraded {
		t.Fatalf("seq 3 status = %s, want degraded (c isolated)", results[3].Status)
	}
	if math.Abs(results[3].Value-10) > 1e-9 {
		t.Fatalf("degraded value = %v, want 10", results[3].Value)
	}
	if results[4].Status != StatusFailed || !math.IsNaN(results[4].Value) {
		t.Fatalf("seq 4 got (%v,%s), want failed/NaN", results[4].Value, results[4].Status)
	}
}

func TestReplayRecoveryReintegration(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs) // RecoverAfter=3

	p := newPipeline(cfg)
	run := func(seq int64, vals map[ChannelID]float64) {
		frame := map[ChannelID]Sample{}
		for _, ch := range chs {
			frame[ch] = Sample{Channel: ch, Seq: seq, Value: vals[ch]}
		}
		_, _ = p.tick(seq, frame)
	}

	// c 漂移被隔离
	for seq := int64(0); seq < 5; seq++ {
		run(seq, map[ChannelID]float64{"a": 10, "b": 10, "c": 14})
	}
	if p.statuses["c"].State != StateFaulty {
		t.Fatalf("c = %s, want faulty", p.statuses["c"].State)
	}

	// c 恢复正确读数，需 RecoverAfter 个连续匹配拍才重新纳入
	for seq := int64(5); seq < 8; seq++ {
		run(seq, map[ChannelID]float64{"a": 10, "b": 10, "c": 10})
	}
	if p.statuses["c"].State != StateHealthy {
		t.Fatalf("c after recovery = %s, want healthy", p.statuses["c"].State)
	}
}

func TestReplayDeterministic(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)
	seqs := []map[ChannelID]float64{
		{"a": 10, "b": 10.2, "c": 13},
		{"a": 10, "b": 10.1, "c": 13.5},
		{"a": 9.9, "b": 10, "c": 14},
		{"a": 10, "b": 10, "c": 14},
	}
	all := sequence(chs, seqs)

	first, err := Replay(cfg, all)
	if err != nil {
		t.Fatal(err)
	}
	// 打乱输入顺序（通道维度），结果必须逐条一致
	shuffled := make([]Sample, len(all))
	copy(shuffled, all)
	for i := len(shuffled) - 1; i > 0; i-- {
		j := (i * 7) % (i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	second, err := Replay(cfg, shuffled)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != len(second) {
		t.Fatalf("length mismatch %d vs %d", len(first), len(second))
	}
	for i := range first {
		f, s := first[i], second[i]
		if f.Seq != s.Seq || f.Status != s.Status {
			t.Fatalf("seq %d mismatch: %+v vs %+v", i, f, s)
		}
		fv, sv := f.Value, s.Value
		if math.IsNaN(fv) != math.IsNaN(sv) || (!math.IsNaN(fv) && fv != sv) {
			t.Fatalf("seq %d value mismatch %v vs %v", i, fv, sv)
		}
	}
}
