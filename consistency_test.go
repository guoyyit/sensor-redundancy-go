package sensor

import (
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// mustSubmit 提交样本并要求成功。
func mustSubmit(t *testing.T, e *Engine, ch ChannelID, seq int64, value float64) {
	t.Helper()
	if err := e.Submit(Sample{Channel: ch, Seq: seq, Value: value}); err != nil {
		t.Fatalf("submit %s@%d: %v", ch, seq, err)
	}
}

// collectResults 后台收集表决结果直到结果管道关闭。
func collectResults(e *Engine) (func() []VoteResult, func()) {
	var mu sync.Mutex
	var out []VoteResult
	done := make(chan struct{})
	go func() {
		defer close(done)
		for r := range e.Results() {
			mu.Lock()
			out = append(out, r)
			mu.Unlock()
		}
	}()
	return func() []VoteResult {
		mu.Lock()
		defer mu.Unlock()
		return append([]VoteResult(nil), out...)
	}, func() { <-done }
}

// 任一时刻读取到的状态必须完整属于同一采样时刻：
// 持续写入与并发读取全过程中，Seq、表决结果、健康状态不得跨拍混合。
func TestSnapshotNeverMixesTicks(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	e, err := NewEngine(DefaultConfig(chs), "")
	if err != nil {
		t.Fatal(err)
	}

	const frames = 200
	var producers sync.WaitGroup
	producers.Add(1)
	go func() {
		defer producers.Done()
		for seq := int64(0); seq < frames; seq++ {
			for _, ch := range chs {
				// 所有通道上报相同数值 seq：表决结果必然等于 seq，
				// 任何跨拍混合都会体现为数值与序号不符。
				_ = e.Submit(Sample{Channel: ch, Seq: seq, Value: float64(seq)})
			}
		}
	}()

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			lastSeq := int64(-1)
			for {
				select {
				case <-stop:
					return
				default:
				}
				v := e.Snapshot()
				if v.Seq != v.Result.Seq {
					t.Errorf("mixed ticks: view seq %d but result seq %d", v.Seq, v.Result.Seq)
					return
				}
				if len(v.Statuses) != len(chs) {
					t.Errorf("partial status set: %d channels", len(v.Statuses))
					return
				}
				if v.Seq < lastSeq {
					t.Errorf("snapshot seq went backwards: %d -> %d", lastSeq, v.Seq)
					return
				}
				lastSeq = v.Seq
				if v.Seq >= 0 {
					if math.Abs(v.Result.Value-float64(v.Seq)) > 1e-9 {
						t.Errorf("view seq %d carries result value %v: cross-tick mix", v.Seq, v.Result.Value)
						return
					}
					for ch, st := range v.Statuses {
						if st.State != StateHealthy {
							t.Errorf("channel %s unexpectedly %s", ch, st.State)
							return
						}
					}
				}
			}
		}()
	}

	producers.Wait()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	close(stop)
	readers.Wait()

	final := e.Snapshot()
	if final.Seq != frames-1 || final.Result.Seq != frames-1 {
		t.Fatalf("final view seqs = (%d,%d), want %d", final.Seq, final.Result.Seq, frames-1)
	}
}

// 已封闭时刻的迟到、重复、乱序样本必须确定性丢弃；
// 同一时刻同一通道重复上报时只有首次接收的读数生效。
func TestLateDuplicateOutOfOrderDropped(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	e, err := NewEngine(DefaultConfig(chs), "")
	if err != nil {
		t.Fatal(err)
	}

	// seq 0 完整帧，等待其封闭。
	mustSubmit(t, e, "a", 0, 10)
	mustSubmit(t, e, "b", 0, 10)
	mustSubmit(t, e, "c", 0, 10)
	r0 := waitResult(t, e)
	if r0.Seq != 0 || r0.Value != 10 {
		t.Fatalf("seq0 result = %+v", r0)
	}

	// 已封闭时刻的迟到样本：全部丢弃，不得再次产出 seq 0 的结果。
	mustSubmit(t, e, "a", 0, 99)
	mustSubmit(t, e, "b", 0, 99)
	mustSubmit(t, e, "c", 0, 99)

	// seq 1：a 重复上报，首次读数 11 生效，99 被丢弃。
	mustSubmit(t, e, "a", 1, 11)
	mustSubmit(t, e, "a", 1, 99)
	mustSubmit(t, e, "b", 1, 11)
	mustSubmit(t, e, "c", 1, 11)
	r1 := waitResult(t, e)
	if r1.Seq != 1 || r1.Value != 11 {
		t.Fatalf("seq1 result = %+v, want value 11 (first sample wins)", r1)
	}

	// 乱序的旧时刻样本同样丢弃。
	mustSubmit(t, e, "a", 0, 5)

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	st := e.Stats()
	if st.AcceptedSamples != 11 {
		t.Fatalf("accepted = %d, want 11", st.AcceptedSamples)
	}
	if st.ProcessedSamples != 6 {
		t.Fatalf("processed = %d, want 6 (two full frames)", st.ProcessedSamples)
	}
	if st.DroppedSamples != 5 {
		t.Fatalf("dropped = %d, want 5 (3 late + 1 duplicate + 1 out-of-order)", st.DroppedSamples)
	}
	if st.EmittedResults != 2 {
		t.Fatalf("emitted = %d, want 2 (closed ticks never re-emit)", st.EmittedResults)
	}
	if st.AcceptedSamples != st.ProcessedSamples+st.DroppedSamples {
		t.Fatalf("reconciliation failed: %+v", st)
	}
}

// 已隔离通道的恢复只能依据新到达的连续采样时刻推进：
// 重复投递同一时刻的旧读数不得计入连续匹配，不得让通道提前恢复。
func TestIsolatedChannelOldReadsDoNotRecover(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	e, err := NewEngine(DefaultConfig(chs), "")
	if err != nil {
		t.Fatal(err)
	}

	// c 持续漂移，seq 0..4 后被隔离（FaultAfter=3）。
	for seq := int64(0); seq < 5; seq++ {
		submitFrame(t, e, chs, seq, map[ChannelID]float64{"a": 10, "b": 10, "c": 14})
		waitResult(t, e)
	}
	if st := e.Snapshot().Statuses["c"]; st.State != StateFaulty {
		t.Fatalf("precondition: c = %s, want faulty", st.State)
	}

	// 重复投递已封闭时刻的“好读数”：不得计入恢复窗口。
	for round := 0; round < 3; round++ {
		for seq := int64(0); seq < 5; seq++ {
			submitFrame(t, e, chs, seq, map[ChannelID]float64{"a": 10, "b": 10, "c": 10})
		}
	}
	st := e.Snapshot().Statuses["c"]
	if st.State != StateFaulty || st.MatchStreak != 0 {
		t.Fatalf("old reads advanced recovery: %+v", st)
	}
	if got := e.Stats().EmittedResults; got != 5 {
		t.Fatalf("old reads produced results: emitted = %d, want 5", got)
	}

	// 新时刻的连续好读数才能推进恢复：RecoverAfter=3。
	for seq := int64(5); seq < 8; seq++ {
		submitFrame(t, e, chs, seq, map[ChannelID]float64{"a": 10, "b": 10, "c": 10})
		waitResult(t, e)
	}
	if st := e.Snapshot().Statuses["c"]; st.State != StateHealthy {
		t.Fatalf("c after genuine recovery window = %s, want healthy", st.State)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}

// assertSameResults 要求两条结果序列逐条一致（NaN 安全）。
func assertSameResults(t *testing.T, got, want []VoteResult) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("result count %d != %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Seq != w.Seq || g.Status != w.Status || g.Reason != w.Reason {
			t.Fatalf("result %d: %+v != %+v", i, g, w)
		}
		if math.IsNaN(g.Value) != math.IsNaN(w.Value) || (!math.IsNaN(g.Value) && g.Value != w.Value) {
			t.Fatalf("result %d value %v != %v", i, g.Value, w.Value)
		}
		if len(g.Contributors) != len(w.Contributors) {
			t.Fatalf("result %d contributors %v != %v", i, g.Contributors, w.Contributors)
		}
		for j := range g.Contributors {
			if g.Contributors[j] != w.Contributors[j] {
				t.Fatalf("result %d contributors %v != %v", i, g.Contributors, w.Contributors)
			}
		}
	}
}

// 并发提交与停机交错：已接受样本全部处理完毕，停机期间的提交
// 要么被完整处理，要么被明确拒绝（ErrClosed），不得静默丢弃。
func TestCloseDrainsConcurrentSubmits(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	e, err := NewEngine(DefaultConfig(chs), "")
	if err != nil {
		t.Fatal(err)
	}

	const frames = 30
	var accepted int64
	var mu sync.Mutex
	var producers sync.WaitGroup
	for _, ch := range chs {
		producers.Add(1)
		go func(ch ChannelID) {
			defer producers.Done()
			for seq := int64(0); seq < frames; seq++ {
				err := e.Submit(Sample{Channel: ch, Seq: seq, Value: float64(seq)})
				if err == ErrClosed {
					return // 明确拒绝：停止本通道提交
				}
				if err != nil {
					t.Errorf("unexpected submit error: %v", err)
					return
				}
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}(ch)
	}

	// 立即发起停机，与并发提交交错。
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	producers.Wait()

	st := e.Stats()
	mu.Lock()
	if accepted != st.AcceptedSamples {
		t.Fatalf("accepted mismatch: submitters saw %d, stats %d", accepted, st.AcceptedSamples)
	}
	mu.Unlock()
	if st.AcceptedSamples != st.ProcessedSamples+st.DroppedSamples {
		t.Fatalf("reconciliation failed: %+v", st)
	}
	if st.DroppedSamples != 0 {
		t.Fatalf("unexpected drops with well-formed producers: %+v", st)
	}
	// 停机前排空：结果管道中应能读出全部产出（缓冲覆盖突发）。
	n := 0
	for range e.Results() {
		n++
	}
	if int64(n) != st.EmittedResults {
		t.Fatalf("drained %d results, stats emitted %d", n, st.EmittedResults)
	}

	// 停机后的提交被明确拒绝。
	if err := e.Submit(Sample{Channel: "a", Seq: 100, Value: 1}); err != ErrClosed {
		t.Fatalf("submit after close = %v, want ErrClosed", err)
	}
	// 重复关闭安全返回。
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}

// 停机排空后重启：从最后对外可见时刻衔接，不重复封闭时刻，
// 恢复状态与停机前最后可见状态一致，且与离线回放逐条一致。
func TestDrainRestartContinues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)

	var samples []Sample
	for seq := int64(0); seq < 10; seq++ {
		for _, ch := range chs {
			samples = append(samples, Sample{Channel: ch, Seq: seq, Value: float64(seq)})
		}
	}

	e1, err := NewEngine(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	get1, wait1 := collectResults(e1)
	for _, s := range samples {
		if err := e1.Submit(s); err != nil {
			t.Fatal(err)
		}
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}
	wait1()
	online := get1()
	if len(online) != 10 {
		t.Fatalf("online produced %d results, want 10", len(online))
	}
	final1 := e1.Snapshot()
	if final1.Seq != 9 || final1.Result.Seq != 9 || final1.Result.Value != 9 {
		t.Fatalf("final view before restart = %+v", final1)
	}

	// 离线回放同一序列：逐条一致。
	replayed, err := Replay(cfg, samples)
	if err != nil {
		t.Fatal(err)
	}
	assertSameResults(t, online, replayed)

	// 重启：状态与停机前最后可见状态一致。
	e2, err := NewEngine(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	view := e2.Snapshot()
	if view.Seq != final1.Seq || view.Result.Seq != final1.Result.Seq ||
		view.Result.Value != final1.Result.Value || view.Result.Status != final1.Result.Status {
		t.Fatalf("restored view %+v != pre-shutdown %+v", view, final1)
	}
	for ch, st := range view.Statuses {
		if st != final1.Statuses[ch] {
			t.Fatalf("channel %s status %+v != %+v", ch, st, final1.Statuses[ch])
		}
	}

	// 已封闭时刻不得重复处理：重投 seq 9 被确定性丢弃。
	submitFrame(t, e2, chs, 9, map[ChannelID]float64{"a": 99, "b": 99, "c": 99})
	// 新时刻正常衔接。
	submitFrame(t, e2, chs, 10, map[ChannelID]float64{"a": 10, "b": 10, "c": 10})
	r := waitResult(t, e2)
	if r.Seq != 10 || r.Value != 10 {
		t.Fatalf("post-restart result = %+v, want seq 10 value 10", r)
	}
	st := e2.Stats()
	if st.DroppedSamples != 3 {
		t.Fatalf("re-submitted closed tick not dropped: %+v", st)
	}
	if st.EmittedResults != 1 {
		t.Fatalf("closed tick re-emitted: %+v", st)
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
}

// 相同采样序列与配置下，在线运行、停机排空与离线回放逐条一致；
// 相同输入重复执行多次，结果完全一致。
func TestOnlineDrainReplayEquivalent(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)

	// c 在 seq 3..8 缺报，seq 9 回报：水位规则将 3..8 关闭为 {a,b} 降级帧。
	var samples []Sample
	for seq := int64(0); seq < 10; seq++ {
		samples = append(samples,
			Sample{Channel: "a", Seq: seq, Value: 10},
			Sample{Channel: "b", Seq: seq, Value: 10})
		if seq < 3 || seq == 9 {
			samples = append(samples, Sample{Channel: "c", Seq: seq, Value: 10})
		}
	}

	runOnline := func() []VoteResult {
		e, err := NewEngine(cfg, "")
		if err != nil {
			t.Fatal(err)
		}
		get, wait := collectResults(e)
		for _, s := range samples {
			if err := e.Submit(s); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		wait()
		return get()
	}

	first := runOnline()
	second := runOnline()
	assertSameResults(t, second, first)

	if len(first) != 10 {
		t.Fatalf("got %d results, want 10", len(first))
	}
	for i, r := range first {
		if r.Seq != int64(i) {
			t.Fatalf("result %d has seq %d", i, r.Seq)
		}
		switch {
		case r.Seq < 3 || r.Seq == 9:
			if r.Status != StatusOK || len(r.Contributors) != 3 {
				t.Fatalf("seq %d = (%s,%v), want full ok frame", r.Seq, r.Status, r.Contributors)
			}
		default:
			if r.Status != StatusDegraded || len(r.Contributors) != 2 ||
				r.Contributors[0] != "a" || r.Contributors[1] != "b" {
				t.Fatalf("seq %d = (%s,%v), want degraded {a,b}", r.Seq, r.Status, r.Contributors)
			}
			if r.Value != 10 {
				t.Fatalf("seq %d degraded value = %v, want 10", r.Seq, r.Value)
			}
		}
	}

	replayed, err := Replay(cfg, samples)
	if err != nil {
		t.Fatal(err)
	}
	assertSameResults(t, first, replayed)
}

// 下游暂时不读取时写入不得阻塞，内部状态与内存有界。
func TestSlowConsumerDoesNotBlockWrites(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	e, err := NewEngine(DefaultConfig(chs), "")
	if err != nil {
		t.Fatal(err)
	}

	const frames = 100
	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := int64(0); seq < frames; seq++ {
			for _, ch := range chs {
				_ = e.Submit(Sample{Channel: ch, Seq: seq, Value: float64(seq)})
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("writes blocked while consumer was not reading")
	}

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		_ = e.Close()
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("close blocked while consumer was not reading")
	}

	st := e.Stats()
	if st.AcceptedSamples != 3*frames || st.ProcessedSamples != 3*frames ||
		st.DroppedSamples != 0 || st.EmittedResults != frames {
		t.Fatalf("unexpected stats: %+v", st)
	}
	// 未读取的结果被有界缓冲吸收，多余的最旧结果被丢弃：内存有界。
	if n := len(e.Results()); n > cap(e.Results()) {
		t.Fatalf("results channel over capacity: %d", n)
	}
}

// 单通道长时间缺报：其余通道按确定规则继续推进（积压上限强制关闭），
// 降级输出与实际到位通道集合一致，且可复现、可回放。
func TestSilentChannelProgressesDeterministically(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	cfg := DefaultConfig(chs)
	cfg.MaxPendingFrames = 3

	var samples []Sample
	for seq := int64(0); seq < 13; seq++ {
		samples = append(samples,
			Sample{Channel: "a", Seq: seq, Value: 10},
			Sample{Channel: "b", Seq: seq, Value: 10})
		if seq < 3 {
			samples = append(samples, Sample{Channel: "c", Seq: seq, Value: 10})
		}
	}

	runOnline := func() []VoteResult {
		e, err := NewEngine(cfg, "")
		if err != nil {
			t.Fatal(err)
		}
		get, wait := collectResults(e)
		for _, s := range samples {
			if err := e.Submit(s); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		wait()
		return get()
	}

	first := runOnline()
	assertSameResults(t, runOnline(), first)

	if len(first) != 13 {
		t.Fatalf("got %d results, want 13 (no unbounded backlog)", len(first))
	}
	for i, r := range first {
		if r.Seq != int64(i) {
			t.Fatalf("result %d has seq %d", i, r.Seq)
		}
		if r.Seq < 3 {
			if r.Status != StatusOK {
				t.Fatalf("seq %d = %s, want ok", r.Seq, r.Status)
			}
			continue
		}
		// c 缺报的帧：按实际到位的 {a,b} 降级输出，可解释。
		if r.Status != StatusDegraded || r.Value != 10 ||
			len(r.Contributors) != 2 || r.Contributors[0] != "a" || r.Contributors[1] != "b" {
			t.Fatalf("seq %d = (%s,%v,%v), want degraded {a,b} 10", r.Seq, r.Status, r.Value, r.Contributors)
		}
	}

	replayed, err := Replay(cfg, samples)
	if err != nil {
		t.Fatal(err)
	}
	assertSameResults(t, first, replayed)
}

// 在线更新配置不得重置已发布状态：视图仍属于同一采样时刻，
// 最后表决结果与最近可靠参考原样保留。
func TestUpdateConfigKeepsViewConsistent(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	e, err := NewEngine(DefaultConfig(chs), "")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	for seq := int64(0); seq < 3; seq++ {
		submitFrame(t, e, chs, seq, map[ChannelID]float64{"a": 10, "b": 10, "c": 10})
		waitResult(t, e)
	}
	before := e.Snapshot()

	cfg2 := DefaultConfig(chs)
	cfg2.DriftThreshold = 2.5
	cfg2.MaxPendingFrames = 16
	if err := e.UpdateConfig(cfg2); err != nil {
		t.Fatal(err)
	}

	after := e.Snapshot()
	if after.Seq != before.Seq || after.Result.Seq != before.Result.Seq ||
		after.Result.Value != before.Result.Value || after.Result.Status != before.Result.Status {
		t.Fatalf("config update mixed the view: %+v -> %+v", before, after)
	}
	if after.Seq != after.Result.Seq {
		t.Fatalf("view spans ticks after update: seq %d vs result seq %d", after.Seq, after.Result.Seq)
	}

	// 新参数（含积压上限）生效，处理继续推进。
	submitFrame(t, e, chs, 3, map[ChannelID]float64{"a": 10, "b": 10, "c": 10})
	r := waitResult(t, e)
	if r.Seq != 3 || r.Status != StatusOK {
		t.Fatalf("tick after update = %+v", r)
	}
}

// 失效拍（NaN）也能完整落盘并在重启后原样恢复：
// 最后可见状态包含失效结果本身。
func TestRestartRestoresFailedTickResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	chs := []ChannelID{"a", "b"}
	cfg := DefaultConfig(chs)

	e1, err := NewEngine(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	// 双余度分歧：明确的失效拍，值为 NaN。
	submitFrame(t, e1, chs, 0, map[ChannelID]float64{"a": 10, "b": 10})
	if r := waitResult(t, e1); r.Status != StatusOK {
		t.Fatalf("seq0 = %s, want ok", r.Status)
	}
	submitFrame(t, e1, chs, 1, map[ChannelID]float64{"a": 10, "b": 20})
	r := waitResult(t, e1)
	if r.Status != StatusFailed || !math.IsNaN(r.Value) {
		t.Fatalf("seq1 = (%v,%s), want failed/NaN", r.Value, r.Status)
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := NewEngine(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	view := e2.Snapshot()
	if view.Seq != 1 || view.Result.Seq != 1 || view.Result.Status != StatusFailed ||
		!math.IsNaN(view.Result.Value) {
		t.Fatalf("restored failed-tick view = %+v", view)
	}
}
