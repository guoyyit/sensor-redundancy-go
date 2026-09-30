package sensor

import (
	"math"
	"sync"
	"testing"
	"time"
)

func drainResults(t *testing.T, e *Engine, n int) []VoteResult {
	t.Helper()
	out := make([]VoteResult, 0, n)
	timeout := time.After(3 * time.Second)
	for len(out) < n {
		select {
		case r := <-e.Results():
			out = append(out, r)
		case <-timeout:
			t.Fatalf("timeout: got %d/%d results", len(out), n)
		}
	}
	return out
}

func TestEngineConcurrentWritersNoCrossFrameMix(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	e, err := NewEngine(DefaultConfig(chs), "")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	const n = 50
	var wg sync.WaitGroup
	for _, ch := range chs {
		wg.Add(1)
		go func(ch ChannelID) {
			defer wg.Done()
			for seq := int64(0); seq < n; seq++ {
				_ = e.Submit(Sample{Channel: ch, Seq: seq, At: time.Now(), Value: float64(seq)})
			}
		}(ch)
	}

	results := drainResults(t, e, n)
	wg.Wait()

	for i, r := range results {
		if r.Seq != int64(i) {
			t.Fatalf("result %d has seq %d (out-of-order frame)", i, r.Seq)
		}
		if r.Status == StatusFailed {
			t.Fatalf("seq %d failed unexpectedly", r.Seq)
		}
		if math.Abs(r.Value-float64(i)) > 1e-9 {
			t.Fatalf("seq %d value %v != %v: cross-frame mixing", i, r.Value, float64(i))
		}
	}
}

func TestEngineSnapshotSelfConsistent(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	e, err := NewEngine(DefaultConfig(chs), "")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for seq := int64(0); seq < 100; seq++ {
			for _, ch := range chs {
				_ = e.Submit(Sample{Channel: ch, Seq: seq, Value: float64(seq)})
			}
		}
	}()

	// 并发读取：任一 Snapshot 的状态数必须等于通道数，且 Seq 单调。
	lastSeq := int64(-1)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		v := e.Snapshot()
		if len(v.Statuses) != len(chs) {
			t.Fatalf("partial status set: %d channels", len(v.Statuses))
		}
		if v.Seq < lastSeq {
			t.Fatalf("snapshot seq went backwards: %d -> %d", lastSeq, v.Seq)
		}
		lastSeq = v.Seq
	}
	wg.Wait()
}

func TestEngineLateSampleDiscarded(t *testing.T) {
	chs := []ChannelID{"a", "b", "c"}
	e, err := NewEngine(DefaultConfig(chs), "")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// 完整送 seq 0 触发其关闭；之后迟到的 seq 0 读数必须被丢弃。
	for _, ch := range chs {
		_ = e.Submit(Sample{Channel: ch, Seq: 0, Value: 0})
	}
	_ = e.Submit(Sample{Channel: "a", Seq: 0, Value: 99})

	r0 := <-e.Results()
	if r0.Seq != 0 || r0.Value != 0 {
		t.Fatalf("first result = %+v", r0)
	}
}
