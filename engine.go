package sensor

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

// Stats 是引擎运行期计数，用于停机排空语义的对账。
// 不变式：Accepted == Dropped + SamplesProcessed；
// FramesProcessed 等于产出的表决结果数；
// 结果通道实际投递数 == FramesProcessed - ResultsDropped。
type Stats struct {
	// Accepted 是 Submit 成功接受（返回 nil）的样本数。
	Accepted int64
	// Dropped 是被收集器确定性丢弃的样本数（迟到、重复、乱序）。
	Dropped int64
	// SamplesProcessed 是进入已关闭帧并参与表决的样本数。
	SamplesProcessed int64
	// FramesProcessed 是已关闭并完成表决的帧数，即产出的结果数。
	FramesProcessed int64
	// ResultsDropped 是因结果通道缓冲满（消费者缓慢）被丢弃的结果数。
	ResultsDropped int64
}

// Engine 并发采集多路采样，逐拍完成检测、表决、健康迁移与持久化。
type Engine struct {
	// mu 保护状态视图（cfg/pipe/seq）：Snapshot 与 Submit 互不阻塞。
	mu sync.RWMutex
	// acceptMu 保护接受生命周期（closed/inflight），临界区极小，
	// 停机判定与样本接受互斥，保证停机期间并发提交得到明确结果。
	acceptMu sync.Mutex

	cfg       Config
	statePath string

	pipe *pipeline
	seq  int64
	// closedSeq 是已封闭的最大采样序号（全新引擎为 -1），
	// 用于重启后丢弃已封闭时刻的重复样本。
	closedSeq int64

	input   chan Sample
	results chan VoteResult
	events  chan TransitionEvent

	closed   bool
	inflight sync.WaitGroup
	wg       sync.WaitGroup

	// frameLimit 是待处理帧跨度上限，运行期可调（见 UpdateConfig）。
	frameLimit atomic.Int64

	accepted         atomic.Int64
	dropped          atomic.Int64
	samplesProcessed atomic.Int64
	framesProcessed  atomic.Int64
	resultsDropped   atomic.Int64

	// persistErr 记录最近一次落盘错误；仅后台 goroutine 写，
	// Close 在 wg.Wait 之后读，无数据竞争。
	persistErr error
}

// 输出管道缓冲：足以吸收突发产出，慢消费者超出缓冲时按丢最旧策略处理，
// 保证写入侧永不阻塞且内存有界。
const ioBufferSize = 256

// NewEngine 创建引擎；statePath 非空时启用崩溃恢复。
func NewEngine(cfg Config, statePath string) (*Engine, error) {
	cfg = cfg.clone()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.normalized()
	e := &Engine{
		cfg:       cfg,
		statePath: statePath,
		pipe:      newPipeline(cfg),
		seq:       0,
		closedSeq: -1,
		input:     make(chan Sample, len(cfg.Channels)*4),
		results:   make(chan VoteResult, ioBufferSize),
		events:    make(chan TransitionEvent, ioBufferSize),
	}
	e.frameLimit.Store(int64(cfg.MaxPendingFrames))
	if err := e.restore(); err != nil {
		return nil, err
	}

	e.wg.Add(1)
	go e.run(cfg.Channels, e.closedSeq)
	return e, nil
}

// Submit 把一条采样写入内存输入管道，支持多生产者并发调用。
// 返回 nil 表示样本已被接受，停机排空时保证被完整处理；
// 引擎已关闭或通道未知时返回明确错误，绝不静默丢弃。
func (e *Engine) Submit(s Sample) error {
	if !e.knownChannel(s.Channel) {
		return fmt.Errorf("sensor: unknown channel %q", s.Channel)
	}
	e.acceptMu.Lock()
	if e.closed {
		e.acceptMu.Unlock()
		return errors.New("sensor: engine closed")
	}
	e.inflight.Add(1)
	e.acceptMu.Unlock()
	defer e.inflight.Done()

	e.accepted.Add(1)
	e.input <- s
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

// Snapshot 读取当前完整自洽状态；永远对应某个已完成采样拍：
// 采样序号、表决结果与各通道健康状态在同一把锁内更新，
// 读取方不会看到来自相邻两拍的混合视图。
func (e *Engine) Snapshot() StateView {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return StateView{
		Seq:      e.seq,
		Result:   e.pipe.lastResult,
		Statuses: e.copyStatuses(),
	}
}

// Stats 返回运行期计数快照，供停机排空对账与测试断言。
func (e *Engine) Stats() Stats {
	return Stats{
		Accepted:         e.accepted.Load(),
		Dropped:          e.dropped.Load(),
		SamplesProcessed: e.samplesProcessed.Load(),
		FramesProcessed:  e.framesProcessed.Load(),
		ResultsDropped:   e.resultsDropped.Load(),
	}
}

// UpdateConfig 在线调整阈值与恢复窗口。
// 通道集合不可变更；计数、健康状态与最近表决结果原样保留，
// 下一拍起按新参数判定。
func (e *Engine) UpdateConfig(cfg Config) error {
	cfg = cfg.clone()
	if err := cfg.Validate(); err != nil {
		return err
	}
	cfg = cfg.normalized()

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
	// 最近表决结果与内部参考一并保留：配置切换后 Snapshot 仍对应
	// 同一采样时刻，不会出现跨拍混合或无解释跳变。
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
	e.frameLimit.Store(int64(cfg.MaxPendingFrames))
	return nil
}

// Close 排空输入、停止后台循环，并保证最后状态已落盘。
// 返回后：所有已接受样本均已处理完毕并产出结果，
// 并发 Submit 要么已被完整处理，要么已收到明确拒绝。
func (e *Engine) Close() error {
	e.acceptMu.Lock()
	if e.closed {
		e.acceptMu.Unlock()
		e.wg.Wait()
		return e.persistErr
	}
	e.closed = true
	e.acceptMu.Unlock()

	// 等待所有已接受样本落入管道后再关闭：不会有发送中的样本被遗漏，
	// 也不会再有任何 goroutine 向已关闭管道发送。
	e.inflight.Wait()
	close(e.input)
	e.wg.Wait()
	return e.persistErr
}

// run 是唯一的后台 goroutine：从内存管道汇集多通道采样，
// 按采样序号组装帧并串行喂给无 IO 的 pipeline。
// 多生产者可并发 Submit，但所有状态变更都在此串行发生，
// 保证读取方永远看到某一采样时刻下的完整自洽状态。
// 输入关闭后排空全部已接受样本：逐帧 flush 直到收集器清空。
func (e *Engine) run(channels []ChannelID, closedSeq int64) {
	defer e.wg.Done()
	defer close(e.results)

	collector := newFrameCollector(channels, closedSeq, &e.frameLimit)

	flushFrame := func(frame map[ChannelID]Sample) {
		seq := frameSeq(frame)

		// 状态推进、序号更新与持久化快照在同一临界区内完成：
		// Snapshot 读到的三元组必然来自同一采样拍。
		e.mu.Lock()
		result, events := e.pipe.tick(seq, frame)
		e.seq = seq
		snap := e.pipe.snapshot(seq)
		e.mu.Unlock()

		e.framesProcessed.Add(1)
		e.samplesProcessed.Add(int64(len(frame)))

		// 落盘 IO 在锁外进行；快照已是不可变拷贝，且仅本 goroutine 写盘，
		// 顺序与拍序一致。Close 返回前最后一拍必然已落盘。
		if err := persist(e.statePath, snap); err != nil {
			e.persistErr = err
		}

		e.emitResult(result)
		for _, ev := range events {
			e.emitEvent(ev)
		}
	}

	for {
		s, ok := <-e.input
		if !ok {
			// 输入已关闭且排空：逐帧结算所有仍缓冲的帧，
			// 已接受样本一个不漏地处理完毕。
			for frame := collector.flush(); frame != nil; frame = collector.flush() {
				flushFrame(frame)
			}
			return
		}
		frame, dropped := collector.offer(s)
		if dropped {
			e.dropped.Add(1)
		}
		if frame != nil {
			flushFrame(frame)
		}
	}
}

func (e *Engine) emitResult(result VoteResult) {
	select {
	case e.results <- result:
		return
	default:
	}
	// 缓冲满时丢弃最旧结果，保证慢消费者不阻塞采集；
	// 权威状态与持久化不受影响，消费者可用 Snapshot 追平，
	// 丢弃数量计入 Stats.ResultsDropped 供对账。
	select {
	case <-e.results:
		e.resultsDropped.Add(1)
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
	e.mu.RLock()
	defer e.mu.RUnlock()
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
	// 已对外可见的采样时刻视为封闭：重启后重复投递的旧读数被确定性丢弃。
	e.closedSeq = snap.Seq
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
