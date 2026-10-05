package sensor

// pipeline 是无并发、无 IO 的逐拍状态推进核心。
// Engine 在线串行调用它，Replay 也调用同一个 tick，
// 因此在线运行与历史回放的状态迁移、表决输出逐条一致。
type pipeline struct {
	cfg        Config
	detector   *detector
	voter      *voter
	hist       *history
	statuses   map[ChannelID]ChannelStatus
	lastResult VoteResult
	// lastGoodRef 是最近一次成功表决的参考值；在降级失效拍用作
	// 健康判定的延续基准，避免仅剩的分歧通道无法继续隔离判断。
	lastGoodRef float64
	hasGoodRef  bool
}

func newPipeline(cfg Config) *pipeline {
	p := &pipeline{
		cfg:      cfg,
		detector: newDetector(cfg),
		voter:    newVoter(cfg),
		hist:     newHistory(cfg.HistorySize),
		statuses: make(map[ChannelID]ChannelStatus, len(cfg.Channels)),
		// 尚无已完成采样拍时的占位结果：Seq=-1 与引擎初始序号一致，
		// 保证 Snapshot 读到的 Seq 与 Result.Seq 始终相等。
		lastResult: VoteResult{Seq: -1, Status: StatusFailed, Reason: "no completed tick"},
	}
	for _, ch := range cfg.Channels {
		p.statuses[ch] = newChannelStatus()
	}
	return p
}

// tick 处理同一采样时刻的完整一帧，返回表决结果与逐通道迁移事件。
func (p *pipeline) tick(seq int64, frame map[ChannelID]Sample) (VoteResult, []TransitionEvent) {
	voteFrame := make(map[ChannelID]float64, len(frame))
	for ch, s := range frame {
		if p.statuses[ch].State.available() {
			voteFrame[ch] = s.Value
		}
	}

	result := p.voter.vote(seq, voteFrame, len(p.cfg.Channels))
	hasRef := result.Status != StatusFailed
	ref := result.Value
	if result.Status != StatusFailed {
		p.lastGoodRef = result.Value
		p.hasGoodRef = true
	}
	if !hasRef {
		// 表决无法给出可信输出时不对外报数值，但仍可用“可用通道的多数共识”
		// 作为内部隔离依据：让持续偏离多数的通道被隔离，使系统能够降级到
		// 剩余一致通道并恢复输出。该参考永不进入 VoteResult。
		if fallback, ok := fallbackReference(p.cfg, voteFrame); ok {
			ref = fallback
			hasRef = true
		}
	}
	if !hasRef && p.hasGoodRef {
		// 没有当前多数共识时，沿用最近可靠参考；仅用于内部健康判定，
		// 对外输出仍保持 failed，不会产生无解释的数值跳变。
		ref = p.lastGoodRef
		hasRef = true
	}

	events := make([]TransitionEvent, 0)
	for _, ch := range p.cfg.Channels {
		s, ok := frame[ch]
		if !ok {
			continue
		}

		before := p.statuses[ch].State
		var tr HealthTransition
		var kind FaultKind
		if hasRef {
			det := p.detector.check(s, ref, p.hist.get(ch))
			kind = det.Kind
			st := p.statuses[ch]
			tr = st.observe(det.Matched, p.cfg)
			p.statuses[ch] = st
		} else {
			// 无有效参考拍：不改变健康状态与计数。
			tr = HealthTransition{From: before, To: before}
		}
		if before != tr.To {
			events = append(events, TransitionEvent{
				Seq:        seq,
				Channel:    ch,
				Transition: tr,
				Fault:      kind,
			})
		}

		p.hist.add(s)
	}

	p.lastResult = result
	return result, events
}

// snapshot 收集需要持久化的全部有界状态。
func (p *pipeline) snapshot(seq int64) snapshot {
	statuses := make(map[ChannelID]ChannelStatus, len(p.statuses))
	for ch, st := range p.statuses {
		statuses[ch] = st
	}
	return snapshot{
		Seq:         seq,
		Config:      p.cfg.clone(),
		Statuses:    statuses,
		History:     p.hist.snapshot(p.cfg.Channels),
		LastGoodRef: p.lastGoodRef,
		HasGoodRef:  p.hasGoodRef,
		LastResult:  toResultSnapshot(p.lastResult),
	}
}

// restore 用快照恢复状态，通道集合由调用方保证一致。
// 恢复后最后对外结果与停机前最后一拍一致，重启衔接不重复、不跳拍。
func (p *pipeline) restore(snap snapshot) {
	for _, ch := range p.cfg.Channels {
		if st, ok := snap.Statuses[ch]; ok {
			p.statuses[ch] = st
		}
	}
	p.hist.restore(snap.History)
	p.lastGoodRef = snap.LastGoodRef
	p.hasGoodRef = snap.HasGoodRef
	switch {
	case snap.LastResult != nil:
		p.lastResult = snap.LastResult.toVoteResult()
	case snap.Seq >= 0:
		// 旧版本快照不含最后结果：合成与 Seq 自洽的占位结果，
		// 保证恢复视图不出现跨拍混合。
		p.lastResult = VoteResult{Seq: snap.Seq, Status: StatusFailed, Reason: "restored from snapshot without result"}
	}
}
