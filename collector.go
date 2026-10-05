package sensor

import "sync/atomic"

// frameCollector 把并发到达的单通道采样按采样序号组装成完整帧。
//
// 帧关闭规则（确定性，不依赖到达时延或墙钟）：
//   - 已封闭（已产出表决结果）的采样时刻视为凝固：序号不超过封闭水位的
//     迟到、重复、乱序读数一律确定性丢弃，绝不再次产出该时刻的结果；
//   - 同一时刻同一通道重复上报时，只有首次接收的读数生效；
//   - 对最旧待处理帧，某通道在“帧内已有该拍读数”或“水位已严格越过
//     该序号（确认整拍缺数）”时才算到位；所有通道到位后关闭。
//     水位仅等于该序号却尚未见到读数时继续等待——该读数可能已在管道中，
//     避免把正在并发投递的正常样本误判为缺失；
//   - 待处理跨度（最新已见序号与最旧待处理帧序号之差）超过上限时，
//     最旧帧按实际到位通道确定性结算：单通道长期缺报时其余通道继续推进，
//     待处理状态与内存占用有界，推进规则只依赖采样序号；
//   - 输入结束 flush 时不再等待，按实际收到的样本逐帧关闭（真实缺失降级）。
type frameCollector struct {
	channels  []ChannelID
	required  map[ChannelID]struct{}
	frames    map[int64]map[ChannelID]Sample
	watermark map[ChannelID]int64
	// closed 是已封闭（已产出结果）的最大采样序号；帧始终按升序封闭，
	// 因此该水位单调不减。序号不超过它的样本一律视为迟到。
	closed int64
	// newest 是已接收样本的最大采样序号，用于度量待处理跨度。
	newest int64
	// limit 是允许的最大待处理跨度；<=0 表示不做强制结算。
	// 通过原子指针共享，支持运行期调整。
	limit *atomic.Int64
}

// newFrameCollector 创建收集器。closedSeq 为已封闭的最大序号
// （全新引擎为 -1；崩溃恢复后为快照中最后对外可见的采样序号，
// 保证重启后不会重复处理已经封闭的时刻）。
func newFrameCollector(channels []ChannelID, closedSeq int64, limit *atomic.Int64) *frameCollector {
	req := make(map[ChannelID]struct{}, len(channels))
	for _, ch := range channels {
		req[ch] = struct{}{}
	}
	wm := make(map[ChannelID]int64, len(channels))
	for _, ch := range channels {
		wm[ch] = closedSeq
	}
	return &frameCollector{
		channels:  append([]ChannelID(nil), channels...),
		required:  req,
		frames:    make(map[int64]map[ChannelID]Sample),
		watermark: wm,
		closed:    closedSeq,
		newest:    closedSeq,
		limit:     limit,
	}
}

// offer 放入一条采样；若有关闭的帧则返回该帧（每拍最多返回一次）。
// dropped 为 true 表示该样本被确定性丢弃（迟到或同帧重复）。
func (c *frameCollector) offer(s Sample) (frame map[ChannelID]Sample, dropped bool) {
	if _, known := c.required[s.Channel]; !known {
		return nil, false
	}
	// 已封闭时刻的迟到/重复/乱序读数：确定性丢弃。
	if s.Seq <= c.closed {
		return nil, true
	}
	// 水位始终推进到该通道见过的最大序号（乱序到达也能关闭旧帧）。
	if s.Seq > c.watermark[s.Channel] {
		c.watermark[s.Channel] = s.Seq
	}
	if s.Seq > c.newest {
		c.newest = s.Seq
	}

	f := c.frames[s.Seq]
	if f == nil {
		f = make(map[ChannelID]Sample, len(c.channels))
		c.frames[s.Seq] = f
	}
	// 同一时刻同一通道重复上报：首次接收的读数生效，其余丢弃。
	if _, exists := f[s.Channel]; exists {
		return nil, true
	}
	f[s.Channel] = s

	oldest := c.oldestPending()
	if oldest < 0 {
		return nil, false
	}
	// 所有通道对该帧“到位”（有读数或确认越过）时关闭。
	if c.allAccountedFor(oldest) {
		return c.pop(oldest), false
	}
	// 单通道长期缺报：待处理跨度超限，最旧帧按实际到位通道结算，
	// 以明确的降级/失效结果结束等待，积压有界。
	if limit := c.limit.Load(); limit > 0 && c.newest-oldest > limit {
		return c.pop(oldest), false
	}
	return nil, false
}

// flush 在输入结束时关闭仍缓冲的最旧帧；重复调用直到返回 nil
// 即可排空全部已接受样本。
func (c *frameCollector) flush() map[ChannelID]Sample {
	oldest := c.oldestPending()
	if oldest < 0 {
		return nil
	}
	return c.pop(oldest)
}

// oldestPending 返回当前缓冲中的最小帧序号；无缓冲返回 -1。
func (c *frameCollector) oldestPending() int64 {
	oldest := int64(-1)
	for seq := range c.frames {
		if oldest < 0 || seq < oldest {
			oldest = seq
		}
	}
	return oldest
}

// allAccountedFor 判断所有通道对 seq 帧是否已到位。
func (c *frameCollector) allAccountedFor(seq int64) bool {
	frame := c.frames[seq]
	for _, ch := range c.channels {
		if _, has := frame[ch]; has {
			continue
		}
		// 帧内缺读数：只有该通道已严格越过 seq 才能确认是整拍缺数；
		// 水位恰好等于 seq 时读数可能仍在管道中，必须等待。
		if c.watermark[ch] <= seq {
			return false
		}
	}
	return true
}

// pop 取出指定帧并将其序号纳入封闭水位（帧按升序封闭，水位单调）。
func (c *frameCollector) pop(seq int64) map[ChannelID]Sample {
	frame := c.frames[seq]
	delete(c.frames, seq)
	c.closed = seq
	return frame
}
