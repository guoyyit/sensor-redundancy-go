package sensor

import (
	"sync"
	"sync/atomic"
	"testing"
)

// 需求1：持续写入与并发读取同时进行的整个过程中，任何一次 Snapshot
// 都必须完整属于同一个采样时刻：序号、表决结果、健康状态不得跨拍混合。
func TestConcurrentReadWriteAtomicSnapshot(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)
	// 本测试关注读写原子性：把待处理跨度上限调到大于总拍数，
	// 避免生产者进度倾斜触发强制结算（该机制由专门测试覆盖）。
	cfg.MaxPendingFrames = 1000
	e, err := NewEngine(cfg, "")
	if err != nil {
		t.Fatal(err)
	}

	const n = 100
	var stop atomic.Bool
	var readers sync.WaitGroup

	// 两个并发读取方：每次读取都校验三元组来自同一拍。
	for r := 0; r < 2; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			lastSeq := int64(-1)
			for !stop.Load() {
				v := e.Snapshot()
				if len(v.Statuses) != len(chs) {
					t.Errorf("partial status set: %d channels", len(v.Statuses))
					return
				}
				if v.Result.Seq != v.Seq {
					t.Errorf("mixed view: seq=%d but result.seq=%d", v.Seq, v.Result.Seq)
					return
				}
				if v.Seq < lastSeq {
					t.Errorf("snapshot seq went backwards: %d -> %d", lastSeq, v.Seq)
					return
				}
				lastSeq = v.Seq
				switch v.Result.Status {
				case "":
					// 初始零值视图：尚未有任何拍完成。
					if v.Seq != 0 {
						t.Errorf("empty result with seq=%d", v.Seq)
						return
					}
				case StatusOK, StatusDegraded:
					// 所有通道上报值均等于序号：混合视图必然暴露为
					// 结果值与序号不一致。
					if v.Result.Value != float64(v.Seq) {
						t.Errorf("mixed view: seq=%d but value=%v", v.Seq, v.Result.Value)
						return
					}
				default:
					t.Errorf("unexpected failed vote at seq=%d", v.Seq)
					return
				}
			}
		}()
	}

	// 三个并发写入方，每通道各写 n 拍，读数值等于采样序号。
	var writers sync.WaitGroup
	for _, ch := range chs {
		writers.Add(1)
		go func(ch ChannelID) {
			defer writers.Done()
			for seq := int64(0); seq < n; seq++ {
				if err := e.Submit(Sample{Channel: ch, Seq: seq, Value: float64(seq)}); err != nil {
					t.Errorf("submit: %v", err)
					return
				}
			}
		}(ch)
	}

	// 结果排空：缓冲足够大，不应有丢弃。
	var drained int64
	var drainer sync.WaitGroup
	drainer.Add(1)
	go func() {
		defer drainer.Done()
		for range e.Results() {
			drained++
		}
	}()

	writers.Wait()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	drainer.Wait()
	stop.Store(true)
	readers.Wait()

	if drained != n {
		t.Fatalf("drained %d results, want %d", drained, n)
	}
	st := e.Stats()
	if st.ResultsDropped != 0 {
		t.Fatalf("results dropped with drained consumer: %+v", st)
	}
	if st.Accepted != 3*n || st.SamplesProcessed != 3*n || st.FramesProcessed != n || st.Dropped != 0 {
		t.Fatalf("reconciliation failed: %+v", st)
	}
	if v := e.Snapshot(); v.Seq != n-1 || v.Result.Value != float64(n-1) {
		t.Fatalf("final view = seq %d value %v, want %d/%d", v.Seq, v.Result.Value, n-1, n-1)
	}
}

// 需求2：已封闭时刻的迟到、重复、乱序样本必须被确定性丢弃，
// 不得再次产出该时刻结果；同帧同通道只有首次接收的读数生效。
func TestLateDuplicateOutOfOrderDropped(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	e, err := NewEngine(DefaultConfig(chs), "")
	if err != nil {
		t.Fatal(err)
	}
	sub := func(ch ChannelID, seq int64, v float64) {
		if err := e.Submit(Sample{Channel: ch, Seq: seq, Value: v}); err != nil {
			t.Fatalf("submit %s@%d: %v", ch, seq, err)
		}
	}

	// seq 0 完整到达并封闭。
	sub("a", 0, 0)
	sub("b", 0, 0)
	sub("c", 0, 0)
	if r := waitResult(t, e); r.Seq != 0 || r.Value != 0 {
		t.Fatalf("result0 = %+v", r)
	}

	// 已封闭时刻的迟到样本：丢弃，不得再次产出 seq 0 的结果。
	sub("a", 0, 99)
	sub("b", 0, 99)

	// seq 1 完整到达；随后同帧重复上报 c：首次读数生效，重复被丢弃。
	sub("a", 1, 1)
	sub("b", 1, 1)
	sub("c", 1, 1)
	sub("c", 1, 50)
	if r := waitResult(t, e); r.Seq != 1 || r.Value != 1 {
		t.Fatalf("result1 = %+v (duplicate rewrote published output?)", r)
	}

	// 乱序：a@3 先到，随后 seq 2 完整到达，先封闭 seq 2。
	sub("a", 3, 3)
	sub("a", 2, 2)
	sub("b", 2, 2)
	sub("c", 2, 2)
	if r := waitResult(t, e); r.Seq != 2 || r.Value != 2 {
		t.Fatalf("result2 = %+v (out-of-order broke frame order)", r)
	}
	sub("b", 3, 3)
	sub("c", 3, 3)
	if r := waitResult(t, e); r.Seq != 3 || r.Value != 3 {
		t.Fatalf("result3 = %+v", r)
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	// 封闭后不得再有任何产出（迟到的 seq 0/1 样本不会重产结果）。
	for r := range e.Results() {
		t.Fatalf("unexpected extra result after drain: %+v", r)
	}

	st := e.Stats()
	if st.Accepted != 15 {
		t.Fatalf("accepted = %d, want 15", st.Accepted)
	}
	if st.Dropped != 3 {
		t.Fatalf("dropped = %d, want 3 (two late seq-0, one dup seq-1)", st.Dropped)
	}
	if st.SamplesProcessed != 12 || st.FramesProcessed != 4 {
		t.Fatalf("processed = %d samples / %d frames, want 12/4", st.SamplesProcessed, st.FramesProcessed)
	}
	if st.Accepted != st.Dropped+st.SamplesProcessed {
		t.Fatalf("reconciliation failed: %+v", st)
	}
}

// 需求3：已隔离通道的恢复只能依据新到达的连续采样时刻推进；
// 重复投递同一时刻的旧读数不得计入连续匹配，不得提前重新参与表决。
func TestIsolatedChannelDuplicateOldReadingsIgnored(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	e, err := NewEngine(DefaultConfig(chs), "") // FaultAfter=3, RecoverAfter=3
	if err != nil {
		t.Fatal(err)
	}
	frame := func(seq int64, cv float64) {
		submitFrame(t, e, chs, seq, map[ChannelID]float64{"a": 10, "b": 10, "c": cv})
	}

	// c 连续失配三拍 -> 隔离。
	frame(0, 14)
	waitResult(t, e)
	frame(1, 14)
	waitResult(t, e)
	frame(2, 14)
	waitResult(t, e)
	if st := e.Snapshot().Statuses["c"]; st.State != StateFaulty {
		t.Fatalf("precondition: c = %s, want faulty", st.State)
	}

	// seq 3：c 恢复正确读数 -> recovering，MatchStreak=1。
	frame(3, 10)
	waitResult(t, e)
	if st := e.Snapshot().Statuses["c"]; st.State != StateRecovering || st.MatchStreak != 1 {
		t.Fatalf("after seq3: c = %+v, want recovering/1", st)
	}

	// 重复投递已封闭时刻的旧读数：必须被丢弃，不得计入连续匹配。
	for i := 0; i < 3; i++ {
		if err := e.Submit(Sample{Channel: "c", Seq: 3, Value: 10}); err != nil {
			t.Fatal(err)
		}
	}

	// 用远未来的一拍作为屏障：保证上述重复样本已被收集器消费。
	// 若重复读数被错误计入，MatchStreak 将提前届满使 c 恢复 healthy。
	frame(100, 10)
	r100 := waitResult(t, e)
	if st := e.Snapshot().Statuses["c"]; st.State != StateRecovering || st.MatchStreak != 2 {
		t.Fatalf("duplicate old readings advanced recovery: c = %+v, want recovering/2", st)
	}
	for _, ch := range r100.Contributors {
		if ch == "c" {
			t.Fatal("isolated channel c contributed to vote before recovery window elapsed")
		}
	}

	// 再一拍新时刻：连续匹配届满，c 才重新纳入。
	frame(101, 10)
	waitResult(t, e)
	if st := e.Snapshot().Statuses["c"]; st.State != StateHealthy {
		t.Fatalf("after genuine new tick: c = %s, want healthy", st.State)
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if st := e.Stats(); st.Dropped != 3 {
		t.Fatalf("dropped = %d, want 3 duplicate old readings", st.Dropped)
	}
}
