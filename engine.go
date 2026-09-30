package sensor

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// StateView 是某一采样时刻下完整自洽的只读状态视图。
type StateView struct {
	Seq      int64
	Result   VoteResult
	Statuses map[ChannelID]ChannelStatus
}

// TransitionEvent 记录一拍内某通道的状态迁移，供可解释输出与测试断言。
type TransitionEvent struct {
	Seq        int64
	Channel    ChannelID
	Transition HealthTransition
	Fault      FaultKind
}

// Engine 并发采集多路采样，逐拍完成检测、表决、健康迁移与持久化。
type Engine struct {
	mu sync.Mutex

	cfg       Config
	statePath string

	pipe *pipeline
	seq  int64

	input   chan Sample
	results chan VoteResult
	events  chan TransitionEvent

	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
}

// NewEngine 创建引擎；statePath 非空时启用崩溃恢复。
func NewEngine(cfg Config, statePath string) (*Engine, error) {
	cfg = cfg.clone()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	e := &Engine{
		cfg:       cfg,
		statePath: statePath,
		pipe:      newPipeline(cfg),
		input:     make(chan Sample, len(cfg.Channels)*4),
		results:   make(chan VoteResult, len(cfg.Channels)*4),
		events:    make(chan TransitionEvent, len(cfg.Channels)*8),
	}
	if err := e.restore(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.wg.Add(1)
	go e.run(ctx)
	return e, nil
}

// Submit 把一条采样写入内存输入管道，支持多生产者并发调用。
func (e *Engine) Submit(s Sample) error {
	if !e.knownChannel(s.Channel) {
		return fmt.Errorf("sensor: unknown channel %q", s.Channel)
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errors.New("sensor: engine closed")
	}
	ch := e.input
	e.mu.Unlock()

	ch <- s
	return nil
}

// Results 返回表决结果输出管道。
func (e *Engine) Results() <-chan VoteResult {
	return e.results
}

// Transitions 返回健康状态迁移事件输出管道，便于观察隔离与恢复过程。
func (e *Engine) Transitions() <-chan TransitionEvent {
	return e.events
}

// Snapshot 读取当前完整自洽状态；永远对应某个已完成采样拍。
func (e *Engine) Snapshot() StateView {
	e.mu.Lock()
	defer e.mu.Unlock()
	return StateView{
		Seq:      e.seq,
		Result:   e.pipe.lastResult,
		Statuses: e.copyStatuses(),
	}
}

// UpdateConfig 在线调整阈值与恢复窗口。
// 通道集合不可变更；计数与健康状态原样保留，下一拍起按新参数判定。
func (e *Engine) UpdateConfig(cfg Config) error {
	cfg = cfg.clone()
	if err := cfg.Validate(); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if !sameChannelSet(e.cfg.Channels, cfg.Channels) {
		return errors.New("sensor: channel set cannot be changed at runtime")
	}

	old := e.cfg
	statuses := e.pipe.statuses
	hist := e.pipe.hist

	np := newPipeline(cfg)
	np.statuses = statuses
	if cfg.HistorySize != old.HistorySize {
		nh := newHistory(cfg.HistorySize)
		nh.restore(hist.snapshot(cfg.Channels))
		np.hist = nh
	} else {
		np.hist = hist
	}

	e.cfg = cfg
	e.pipe = np
	return nil
}

// Close 排空输入、停止后台循环，并保证最后状态已落盘。
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	close(e.input)
	cancel := e.cancel
	e.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	e.wg.Wait()
	return nil
}

// run 是唯一的后台 goroutine：从内存管道汇集多通道采样，
// 按采样序号组装帧并串行喂给无 IO 的 pipeline。
// 多生产者可并发 Submit，但所有状态变更都在此串行发生，
// 保证读取方永远看到某一采样时刻下的完整自洽状态。
func (e *Engine) run(ctx context.Context) {
	defer e.wg.Done()
	defer close(e.results)

	collector := newFrameCollector(e.cfg.Channels, e.seq)

	flushFrame := func(frame map[ChannelID]Sample) {
		seq := frameSeq(frame)
		result, events := e.processTick(frame)

		// 持久化在状态更新之后立即发生；channel 发送无需持锁。
		e.mu.Lock()
		e.seq = seq
		_ = persist(e.statePath, e.pipe.snapshot(seq))
		e.mu.Unlock()

		// 快照的 seq/result 在同一把锁内对外可见；
		// result 为值类型，读方不会看到跨拍混合。
		e.emitResult(result)
		for _, ev := range events {
			e.emitEvent(ev)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case s, ok := <-e.input:
			if !ok {
				if frame := collector.flush(); frame != nil {
					flushFrame(frame)
				}
				return
			}
			if frame := collector.offer(s); frame != nil {
				flushFrame(frame)
			}
		}
	}
}

// processTick 在互斥保护下推进一拍。
func (e *Engine) processTick(frame map[ChannelID]Sample) (VoteResult, []TransitionEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.pipe.tick(frameSeq(frame), frame)
}

func (e *Engine) emitResult(result VoteResult) {
	select {
	case e.results <- result:
		return
	default:
	}
	// 缓冲满时丢弃最旧结果，保证慢消费者不阻塞采集；
	// 权威状态与持久化不受影响，消费者可用 Snapshot 追平。
	select {
	case <-e.results:
	default:
	}
	e.results <- result
}

func (e *Engine) emitEvent(ev TransitionEvent) {
	select {
	case e.events <- ev:
	default:
		select {
		case <-e.events:
		default:
		}
		select {
		case e.events <- ev:
		default:
		}
	}
}

func (e *Engine) knownChannel(ch ChannelID) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.pipe.statuses[ch]
	return ok
}

func (e *Engine) copyStatuses() map[ChannelID]ChannelStatus {
	out := make(map[ChannelID]ChannelStatus, len(e.pipe.statuses))
	for ch, st := range e.pipe.statuses {
		out[ch] = st
	}
	return out
}

func (e *Engine) restore() error {
	snap, ok, err := loadSnapshot(e.statePath)
	if err != nil {
		return fmt.Errorf("sensor: restore state: %w", err)
	}
	if !ok {
		return nil
	}
	if !sameChannelSet(snap.Config.Channels, e.cfg.Channels) {
		return errors.New("sensor: persisted channel set does not match runtime config")
	}
	e.pipe.restore(snap)
	e.seq = snap.Seq
	return nil
}

// frameSeq 从帧内任一样本取得采样序号（同一帧 Seq 相同）。
func frameSeq(frame map[ChannelID]Sample) int64 {
	var seq int64
	for _, s := range frame {
		seq = s.Seq
		break
	}
	return seq
}

func sameChannelSet(a, b []ChannelID) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[ChannelID]struct{}, len(a))
	for _, ch := range a {
		set[ch] = struct{}{}
	}
	for _, ch := range b {
		if _, ok := set[ch]; !ok {
			return false
		}
	}
	return true
}
