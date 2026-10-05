package sensor

import (
	"math"
	"path/filepath"
	"sync"
	"testing"
)

// 需求5：正常停机时所有已接受样本全部处理完毕、结果全部产出、
// 最终状态完整落盘；接受数与产出数可对账。
func TestShutdownDrainsAllAccepted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)
	cfg.MaxPendingFrames = 1000 // 排除强制结算干扰，专注排空语义
	e, err := NewEngine(cfg, path)
	if err != nil {
		t.Fatal(err)
	}

	const n = 30
	var wg sync.WaitGroup
	for _, ch := range chs {
		wg.Add(1)
		go func(ch ChannelID) {
			defer wg.Done()
			for seq := int64(0); seq < n; seq++ {
				if err := e.Submit(Sample{Channel: ch, Seq: seq, Value: float64(seq)}); err != nil {
					t.Errorf("submit: %v", err)
					return
				}
			}
		}(ch)
	}

	// 并发读取结果直到管道关闭。
	var mu sync.Mutex
	var got []VoteResult
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		for r := range e.Results() {
			mu.Lock()
			got = append(got, r)
			mu.Unlock()
		}
	}()

	wg.Wait() // 全部样本已被接受
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	reader.Wait()

	st := e.Stats()
	if st.Accepted != 3*n || st.Dropped != 0 || st.SamplesProcessed != 3*n {
		t.Fatalf("sample reconciliation failed: %+v", st)
	}
	if st.FramesProcessed != n {
		t.Fatalf("frames processed = %d, want %d", st.FramesProcessed, n)
	}
	// 结果数对账：投递数 + 丢弃数 == 产出数。
	if int64(len(got))+st.ResultsDropped != st.FramesProcessed {
		t.Fatalf("result reconciliation failed: delivered %d + dropped %d != produced %d",
			len(got), st.ResultsDropped, st.FramesProcessed)
	}
	// 已投递结果必须按采样序号严格递增（每拍至多一条）。
	mu.Lock()
	for i := 1; i < len(got); i++ {
		if got[i].Seq <= got[i-1].Seq {
			t.Fatalf("results out of order: seq %d after %d", got[i].Seq, got[i-1].Seq)
		}
	}
	mu.Unlock()

	// 最终状态完整落盘：最后对外可见序号为 n-1。
	snap, ok, err := loadSnapshot(path)
	if err != nil || !ok {
		t.Fatalf("state not persisted: ok=%v err=%v", ok, err)
	}
	if snap.Seq != n-1 {
		t.Fatalf("persisted seq = %d, want %d", snap.Seq, n-1)
	}

	// 停机后提交必须被明确拒绝。
	if err := e.Submit(Sample{Channel: "a", Seq: 100, Value: 1}); err == nil {
		t.Fatal("submit after close must be rejected")
	}
	// 重复停机是安全的。
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}

// 需求5：停机期间并发提交的样本必须得到明确结果——
// 要么被完整处理（计入对账），要么被明确拒绝，不得静默丢弃或恐慌。
func TestSubmitDuringCloseIsDecisive(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	e, err := NewEngine(DefaultConfig(chs), "")
	if err != nil {
		t.Fatal(err)
	}

	// 先正常处理 5 拍。
	for seq := int64(0); seq < 5; seq++ {
		submitFrame(t, e, chs, seq, map[ChannelID]float64{"a": 1, "b": 1, "c": 1})
		waitResult(t, e)
	}

	// 并发：一个 goroutine 持续提交，主 goroutine 发起停机。
	var succeeded int64
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := int64(0); ; i++ {
			err := e.Submit(Sample{Channel: "a", Seq: 1000 + i, Value: 1})
			if err != nil {
				return // 明确拒绝：引擎已关闭
			}
			mu.Lock()
			succeeded++
			mu.Unlock()
		}
	}()

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	<-done

	mu.Lock()
	extra := succeeded
	mu.Unlock()

	// 对账：被接受的样本一个不漏地进入已关闭帧。
	st := e.Stats()
	if st.Accepted != 15+extra {
		t.Fatalf("accepted = %d, want %d", st.Accepted, 15+extra)
	}
	if st.Accepted != st.Dropped+st.SamplesProcessed {
		t.Fatalf("accepted samples silently lost: %+v", st)
	}
	// 每个被接受的额外样本各自构成一拍（缺通道降级/失效），全部产出。
	if st.FramesProcessed != 5+extra {
		t.Fatalf("frames = %d, want %d (5 normal + %d drained partial)",
			st.FramesProcessed, 5+extra, extra)
	}

	// 停机完成后提交必须被明确拒绝。
	if err := e.Submit(Sample{Channel: "a", Seq: 2000, Value: 1}); err == nil {
		t.Fatal("submit after close must be rejected")
	}
}

// 需求6：正常停机后重启，从最后对外可见的采样时刻继续衔接：
// 状态一致、已封闭时刻不重复处理、未处理样本不跳过。
func TestRestartContinuesFromLastVisible(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)

	e1, err := NewEngine(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	for seq := int64(0); seq < 5; seq++ {
		submitFrame(t, e1, chs, seq, map[ChannelID]float64{"a": 10, "b": 10, "c": 10})
		waitResult(t, e1)
	}
	pre := e1.Snapshot()
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := NewEngine(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	post := e2.Snapshot()

	// 恢复后的状态必须与停机前最后一次对外可见状态一致。
	if post.Seq != pre.Seq {
		t.Fatalf("seq = %d, want %d", post.Seq, pre.Seq)
	}
	if post.Result.Seq != pre.Result.Seq || post.Result.Status != pre.Result.Status ||
		post.Result.Value != pre.Result.Value || post.Result.Reason != pre.Result.Reason {
		t.Fatalf("result view changed across restart: %+v -> %+v", pre.Result, post.Result)
	}
	if len(post.Result.Contributors) != len(pre.Result.Contributors) {
		t.Fatalf("contributors changed across restart: %v -> %v",
			pre.Result.Contributors, post.Result.Contributors)
	}
	for i := range pre.Result.Contributors {
		if post.Result.Contributors[i] != pre.Result.Contributors[i] {
			t.Fatalf("contributors changed across restart: %v -> %v",
				pre.Result.Contributors, post.Result.Contributors)
		}
	}
	for _, ch := range chs {
		if post.Statuses[ch] != pre.Statuses[ch] {
			t.Fatalf("status of %s changed across restart: %+v -> %+v",
				ch, pre.Statuses[ch], post.Statuses[ch])
		}
	}

	// 已封闭时刻的重复样本：接受但确定性丢弃，不得再次产出结果。
	for _, ch := range chs {
		if err := e2.Submit(Sample{Channel: ch, Seq: 3, Value: 10}); err != nil {
			t.Fatal(err)
		}
	}
	// 新时刻正常衔接。
	submitFrame(t, e2, chs, 5, map[ChannelID]float64{"a": 10, "b": 10, "c": 10})
	r := waitResult(t, e2)
	if r.Seq != 5 || r.Value != 10 {
		t.Fatalf("post-restart result = %+v, want seq 5 value 10", r)
	}
	if st := e2.Stats(); st.Dropped != 3 {
		t.Fatalf("dropped = %d, want 3 sealed-seq resubmissions", st.Dropped)
	}
	// 结果管道中不得出现已封闭时刻的重复结果。
	select {
	case dup := <-e2.Results():
		t.Fatalf("sealed seq produced another result: %+v", dup)
	default:
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
}

// 需求7：相同采样序列与相同配置下，在线运行、停机排空与离线回放
// 必须给出逐条一致的表决结果；相同输入重复执行结果完全一致。
func TestOnlineDrainReplayConsistency(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)

	// 场景：健康 -> c 漂移被隔离 -> c 恢复并重新纳入。
	vals := []map[ChannelID]float64{
		{"a": 10, "b": 10, "c": 10},
		{"a": 10, "b": 10, "c": 10},
		{"a": 10, "b": 10, "c": 14},
		{"a": 10, "b": 10, "c": 14},
		{"a": 10, "b": 10, "c": 14},
		{"a": 10, "b": 10, "c": 14},
		{"a": 10, "b": 10, "c": 10},
		{"a": 10, "b": 10, "c": 10},
		{"a": 10, "b": 10, "c": 10},
		{"a": 10, "b": 10, "c": 10},
	}
	var samples []Sample
	for seq, v := range vals {
		samples = append(samples, mkSamples(chs, int64(seq), v)...)
	}

	want, err := Replay(cfg, samples)
	if err != nil {
		t.Fatal(err)
	}

	// 在线运行 + 停机排空，重复三次；结果与迁移事件必须逐条一致。
	var wantEvents []TransitionEvent
	for run := 0; run < 3; run++ {
		e, err := NewEngine(cfg, "")
		if err != nil {
			t.Fatal(err)
		}
		var got []VoteResult
		for seq := range vals {
			submitFrame(t, e, chs, int64(seq), vals[seq])
			got = append(got, waitResult(t, e))
		}
		// 停机排空：所有已接受样本处理完毕后不得再有新增产出。
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		for r := range e.Results() {
			got = append(got, r)
		}
		var events []TransitionEvent
		for {
			select {
			case ev := <-e.Transitions():
				events = append(events, ev)
			default:
				goto drained
			}
		}
	drained:

		assertSameResults(t, want, got)
		if run == 0 {
			wantEvents = events
		} else {
			assertSameEvents(t, wantEvents, events)
		}
	}
}

func assertSameResults(t *testing.T, want, got []VoteResult) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("result count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		w, g := want[i], got[i]
		if w.Seq != g.Seq || w.Status != g.Status || w.Reason != g.Reason {
			t.Fatalf("result %d: %+v != %+v", i, g, w)
		}
		if math.IsNaN(w.Value) != math.IsNaN(g.Value) || (!math.IsNaN(w.Value) && w.Value != g.Value) {
			t.Fatalf("result %d value: %v != %v", i, g.Value, w.Value)
		}
		if len(w.Contributors) != len(g.Contributors) {
			t.Fatalf("result %d contributors: %v != %v", i, g.Contributors, w.Contributors)
		}
		for j := range w.Contributors {
			if w.Contributors[j] != g.Contributors[j] {
				t.Fatalf("result %d contributors: %v != %v", i, g.Contributors, w.Contributors)
			}
		}
	}
}

func assertSameEvents(t *testing.T, want, got []TransitionEvent) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("event count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("event %d: %+v != %+v", i, got[i], want[i])
		}
	}
}
