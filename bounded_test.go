package sensor

import (
	"testing"
	"time"
)

// 需求8：下游缓慢甚至暂时不读取时，写入不得被阻塞，内存占用有界；
// 产出数与被丢弃数必须可以对账。
func TestSlowConsumerBounded(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)
	cfg.MaxPendingFrames = 1000 // 排除强制结算干扰
	e, err := NewEngine(cfg, "")
	if err != nil {
		t.Fatal(err)
	}

	const n = 300 // 超过结果缓冲 256，必然触发丢最旧
	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := int64(0); seq < n; seq++ {
			for _, ch := range chs {
				if err := e.Submit(Sample{Channel: ch, Seq: seq, Value: float64(seq)}); err != nil {
					return
				}
			}
		}
	}()

	// 不读取任何结果：写入也必须在有限时间内全部完成。
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("writes blocked by unread results channel")
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// 停机后排空结果管道：剩余结果 + 已丢弃 == 产出总数。
	var drained int64
	for range e.Results() {
		drained++
	}
	st := e.Stats()
	if st.FramesProcessed != n {
		t.Fatalf("frames processed = %d, want %d", st.FramesProcessed, n)
	}
	if st.Accepted != 3*n || st.SamplesProcessed != 3*n || st.Dropped != 0 {
		t.Fatalf("sample reconciliation failed: %+v", st)
	}
	if drained != ioBufferSize {
		t.Fatalf("drained = %d, want exactly buffer capacity %d", drained, ioBufferSize)
	}
	if st.ResultsDropped != n-ioBufferSize {
		t.Fatalf("results dropped = %d, want %d", st.ResultsDropped, n-ioBufferSize)
	}
	if drained+st.ResultsDropped != st.FramesProcessed {
		t.Fatalf("result reconciliation failed: %d + %d != %d",
			drained, st.ResultsDropped, st.FramesProcessed)
	}
}

// 需求4/8：单条通道长时间缺报时，其余通道的处理必须确定性地继续推进，
// 不得无限积压；缺通道的降级输出与实际到位的通道集合一致且可解释；
// 推进规则只依赖采样序号，重复执行结果完全一致。
func TestStalledChannelForcesDeterministicProgress(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	run := func() []VoteResult {
		cfg := DefaultConfig(chs)
		cfg.MaxPendingFrames = 8 // 小跨度便于测试
		e, err := NewEngine(cfg, "")
		if err != nil {
			t.Fatal(err)
		}
		// c 只上报 seq 0 后彻底缺报；a/b 持续上报 seq 0..29。
		for seq := int64(0); seq < 30; seq++ {
			for _, ch := range []ChannelID{"a", "b"} {
				if err := e.Submit(Sample{Channel: ch, Seq: seq, Value: float64(seq)}); err != nil {
					t.Fatal(err)
				}
			}
			if seq == 0 {
				if err := e.Submit(Sample{Channel: "c", Seq: 0, Value: 0}); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		var got []VoteResult
		for r := range e.Results() {
			got = append(got, r)
		}
		st := e.Stats()
		if st.Accepted != 61 || st.SamplesProcessed != 61 || st.Dropped != 0 {
			t.Fatalf("sample reconciliation failed: %+v", st)
		}
		if st.FramesProcessed != 30 {
			t.Fatalf("frames processed = %d, want 30 (no infinite backlog)", st.FramesProcessed)
		}
		return got
	}

	first := run()
	second := run()
	if len(first) != 30 || len(second) != 30 {
		t.Fatalf("result counts = %d/%d, want 30/30", len(first), len(second))
	}
	for i := range first {
		a, b := first[i], second[i]
		if a.Seq != int64(i) {
			t.Fatalf("result %d has seq %d (frames must close in order)", i, a.Seq)
		}
		if a.Seq != b.Seq || a.Status != b.Status || a.Value != b.Value || a.Reason != b.Reason {
			t.Fatalf("run mismatch at seq %d: %+v vs %+v", i, a, b)
		}
		if len(a.Contributors) != len(b.Contributors) {
			t.Fatalf("contributors mismatch at seq %d", i)
		}
		for j := range a.Contributors {
			if a.Contributors[j] != b.Contributors[j] {
				t.Fatalf("contributors mismatch at seq %d", i)
			}
		}
		if a.Seq == 0 {
			// 唯一完整帧：三通道全到位。
			if a.Status != StatusOK || len(a.Contributors) != 3 {
				t.Fatalf("seq 0 = %+v, want ok with all three channels", a)
			}
			continue
		}
		// c 缺报的每一拍：按实际到位的 {a,b} 降级输出，值等于该拍读数。
		if a.Status != StatusDegraded {
			t.Fatalf("seq %d status = %s, want degraded", a.Seq, a.Status)
		}
		if len(a.Contributors) != 2 || a.Contributors[0] != "a" || a.Contributors[1] != "b" {
			t.Fatalf("seq %d contributors = %v, want [a b]", a.Seq, a.Contributors)
		}
		if a.Value != float64(a.Seq) {
			t.Fatalf("seq %d value = %v, want %v", a.Seq, a.Value, float64(a.Seq))
		}
	}
}

// 需求2：离线回放与在线一致——同一时刻同一通道重复上报时首次读数生效。
func TestReplayFirstWinsOnDuplicates(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)

	samples := []Sample{
		{Channel: "a", Seq: 0, Value: 10},
		{Channel: "b", Seq: 0, Value: 10},
		{Channel: "c", Seq: 0, Value: 10},
		{Channel: "a", Seq: 1, Value: 11},
		{Channel: "a", Seq: 1, Value: 99}, // 重复：必须被忽略
		{Channel: "b", Seq: 1, Value: 11},
		{Channel: "c", Seq: 1, Value: 11},
		{Channel: "c", Seq: 1, Value: 55}, // 重复：必须被忽略
	}
	results, err := Replay(cfg, samples)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	if results[1].Value != 11 {
		t.Fatalf("replay used duplicate reading: value = %v, want 11 (first wins)", results[1].Value)
	}

	// 在线按相同顺序提交：结果必须与回放逐条一致。
	e, err := NewEngine(cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range samples {
		if err := e.Submit(s); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	var online []VoteResult
	for r := range e.Results() {
		online = append(online, r)
	}
	if len(online) != 2 {
		t.Fatalf("online results = %d, want 2", len(online))
	}
	for i := range results {
		if online[i].Seq != results[i].Seq || online[i].Value != results[i].Value ||
			online[i].Status != results[i].Status {
			t.Fatalf("online/replay mismatch at %d: %+v vs %+v", i, online[i], results[i])
		}
	}
	if st := e.Stats(); st.Dropped != 2 {
		t.Fatalf("dropped = %d, want 2 duplicates", st.Dropped)
	}
}
