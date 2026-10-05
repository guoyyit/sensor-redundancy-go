package sensor

import (
	"errors"
	"fmt"
	"sync"
)

// ErrClosed 是引擎关闭后 Submit 得到的明确拒绝错误。
var ErrClosed = errors.New("sensor: engine closed")

// StateView 是某一采样时刻下完整自洽的只读状态视图。
// Seq 与 Result.Seq 必然相等；Seq 为 -1 表示尚无已完成的采样拍。
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

// Stats 是引擎运行计数，用于停机对账与丢失审计。
// 停机排空完成后满足对账关系：
// AcceptedSamples == ProcessedSamples + DroppedSamples。
type Stats struct {
	AcceptedSamples  int64 // Submit 成功接收的样本数
	ProcessedSamples int64 // 已进入闭合帧并完成处理的样本数
	DroppedSamples   int64 // 被确定性丢弃的迟到/重复/乱序样本数
	EmittedResults   int64 // 已产出的表决结果数
}

// Engine 并发采集多路采样，逐拍完成检测、表决、健康迁移与持久化。
type Engine struct {
	mu sync.Mutex

	cfg       Config
	statePath string

	pipe      *pipeline
	collector *frameCollector
	seq       int64 // 最后完成的采样序号；-1 表示尚无已完成的采样拍

	input   chan Sample
	results chan VoteResult
	events  chan TransitionEvent

	submitWG   sync.WaitGroup // 在途 Submit：Close 等待其投递完成
	wg         sync.WaitGroup
	closed     bool
	stats      Stats
	persistErr error
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
		seq:       -1,
		input:     make(chan Sample, len(cfg.Channels)*4),
		// 输出缓冲至少容纳“积压上限+1”条结果：单次 offer 或停机排空
		// 触发的突发关闭不会超过该规模，正常读取的消费者不会丢结果；
		// 缓冲仍然有限，长期不读取时按最旧丢弃保证内存有界。
		results: make(chan VoteResult, outputBuffer(cfg)),
		events:  make(chan TransitionEvent, outputBuffer(cfg)*int64(len(cfg.Channels))),
	}
	if err := e.restore(); err != nil {
		return nil, err
	}
	// 采集器从最后对外可见序号的下一拍开始接收：
	// 已封闭的时刻（含崩溃前已落盘的）不会被重复处理。
	e.collector = newFrameCollector(cfg.Channels, e.seq+1, cfg.maxPendingFrames())

	e.wg.Add(1)
	go e.run()
	return e, nil
}

// Submit 把一条采样写入内存输入管道，支持多生产者并发调用。
// 返回 nil 表示样本已被接受，停机时必然被处理完毕；
// 引擎已关闭时返回 ErrClosed（明确拒绝，绝不静默丢弃）。
func (e *Engine) Submit(s Sample) error {
	if !e.knownChannel(s.Channel) {
		return fmt.Errorf("sensor: unknown channel %q", s.Channel)
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	// 登记在途提交：Close 会等待其投递完成才关闭输入管道，
	// 杜绝“向已关闭管道发送”与“已接受样本被丢失”。
	e.submitWG.Add(1)
	e.mu.Unlock()

	e.input <- s

	e.mu.Lock()
	e.stats.AcceptedSamples++
	e.submitWG.Done()
	e.mu.Unlock()
	return nil
}

// Results 返回表决结果输出管道。
// 管道有界：消费者过慢时丢弃最旧结果以保证采集不阻塞、内存有界；
// 权威状态以 Snapshot 与持久化快照为准，产出总数见 Stats().EmittedResults。
func (e *Engine) Results() <-chan VoteResult {
	return e.results
}

// Transitions 返回健康状态迁移事件输出管道，便于观察隔离与恢复过程。
func (e *Engine) Transitions() <-chan TransitionEvent {
	return e.events
}

// Snapshot 读取当前完整自洽状态；永远对应某个已完成采样拍，
// 采样序号、表决结果与各通道健康状态必然来自同一拍。
func (e *Engine) Snapshot() StateView {
	e.mu.Lock()
	defer e.mu.Unlock()
	return StateView{
		Seq:      e.seq,
		Result:   e.pipe.lastResult,
		Statuses: e.copyStatuses(),
	}
}

// Stats 返回当前运行计数快照。
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stats
}

// UpdateConfig 在线调整阈值、恢复窗口与积压上限。
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
	np.lastResult = e.pipe.lastResult
	np.lastGoodRef = e.pipe.lastGoodRef
	np.hasGoodRef = e.pipe.hasGoodRef
	if cfg.HistorySize != old.HistorySize {
		nh := newHistory(cfg.HistorySize)
		nh.restore(hist.snapshot(cfg.Channels))
		np.hist = nh
	} else {
		np.hist = hist
	}

	e.cfg = cfg
	e.pipe = np
	e.collector.setMaxPending(cfg.maxPendingFrames())
	return nil
}

// Close 排空输入、停止后台循环，并保证最后状态已落盘。
// 所有已被接受的样本全部处理完毕、结果全部产出后才返回；
// 可并发、可重复调用，任何调用者都会等到排空完成。
func (e *Engine) Close() error {
	e.mu.Lock()
	if !e.closed {
		e.closed = true
		e.mu.Unlock()
		// 先等待在途 Submit 全部投递完成，再关闭输入管道：
		// 已接受的样本必然进入排空流程，不会被静默丢弃。
		e.submitWG.Wait()
		close(e.input)
	} else {
		e.mu.Unlock()
	}
	e.wg.Wait()

	e.mu.Lock()
	err := e.persistErr
	e.mu.Unlock()
	return err
}

// run 是唯一的后台 goroutine：从内存管道汇集多通道采样，
// 按采样序号组装帧并串行喂给无 IO 的 pipeline。
// 多生产者可并发 Submit，但所有状态变更都在此串行发生，
// 保证读取方永远看到某一采样时刻下的完整自洽状态。
// 输入管道关闭后排空全部待处理帧再退出：已接受样本一个不落。
func (e *Engine) run() {
	defer e.wg.Done()
	defer close(e.results)
	defer close(e.events)

	for s := range e.input {
		frames, dropped := e.collector.offer(s)
		if dropped {
			e.noteDropped()
		}
		for _, frame := range frames {
			e.flushFrame(frame)
		}
	}
	for _, frame := range e.collector.drain() {
		e.flushFrame(frame)
	}
}

// flushFrame 推进一拍并对外发布：先在锁内完成状态迁移与序号推进，
// 再在锁外落盘与投递结果（IO 不阻塞并发读取）。
func (e *Engine) flushFrame(frame map[ChannelID]Sample) {
	result, events, snap := e.processTick(frame)

	// 快照已在锁内捕获；落盘只发生在 run goroutine，顺序与采样序号一致。
	if err := persist(e.statePath, snap); err != nil {
		e.setPersistErr(err)
	}

	// 快照的 seq/result 已在同一把锁内对外可见；
	// result 为值类型，读方不会看到跨拍混合。
	e.emitResult(result)
	for _, ev := range events {
		e.emitEvent(ev)
	}
}

// processTick 在互斥保护下推进一拍：表决、健康迁移、序号推进与统计
// 在同一把锁内完成，Snapshot 永远不会读到跨拍混合的状态。
func (e *Engine) processTick(frame map[ChannelID]Sample) (VoteResult, []TransitionEvent, snapshot) {
	e.mu.Lock()
	defer e.mu.Unlock()
	seq := frameSeq(frame)
	result, events := e.pipe.tick(seq, frame)
	e.seq = seq
	e.stats.ProcessedSamples += int64(len(frame))
	e.stats.EmittedResults++
	return result, events, e.pipe.snapshot(seq)
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

func (e *Engine) noteDropped() {
	e.mu.Lock()
	e.stats.DroppedSamples++
	e.mu.Unlock()
}

func (e *Engine) setPersistErr(err error) {
	e.mu.Lock()
	if e.persistErr == nil {
		e.persistErr = err
	}
	e.mu.Unlock()
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

// outputBuffer 计算结果输出管道容量：覆盖单帧突发关闭的最大规模。
func outputBuffer(cfg Config) int64 {
	n := int64(cfg.maxPendingFrames()) + 1
	if min := int64(len(cfg.Channels)) * 4; n < min {
		n = min
	}
	return n
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
