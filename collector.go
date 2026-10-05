package sensor

import "sync/atomic"

// frameCollector 把并发到达的单通道采样按采样序号组装成完整帧。
//
// 帧关闭规则（确定性，只取决于样本序列，不依赖墙钟或到达时延）：
//   - 对最旧待处理帧，某通道在“帧内已有该拍读数”或“水位已严格越过
//     该序号（确认整拍缺数）”时才算到位；所有通道到位后关闭。
//     水位仅等于该序号却尚未见到读数时继续等待——该读数可能已在管道中，
//     避免把正在并发投递的正常样本误判为缺失；
//   - 待处理帧数超过上限时，强制关闭最旧帧（按实际到位通道降级输出），
//     保证单通道长期缺报时内存有界、其余通道确定地继续推进；
//   - 输入结束 drain 时不再等待，按实际收到的样本关闭（真实缺失降级）；
//   - 序号不超过 closedUpTo 的读数一律丢弃：该时刻已封闭（已产出结果或
//     已判定整拍缺数），迟到、重复与乱序样本绝不混入已封闭的帧，
//     也不会让同一时刻再次产出结果。
type frameCollector struct {
	channels   []ChannelID
	required   map[ChannelID]struct{}
	frames     map[int64]map[ChannelID]Sample
	watermark  map[ChannelID]int64
	closedUpTo int64
	maxPending atomic.Int64
}

// newFrameCollector 创建组装器；nextSeq 为允许接收的最小采样序号，
// 序号更小的时刻视为已封闭（重启续接时 nextSeq = 最后可见序号+1）。
func newFrameCollector(channels []ChannelID, nextSeq int64, maxPending int) *frameCollector {
	req := make(map[ChannelID]struct{}, len(channels))
	wm := make(map[ChannelID]int64, len(channels))
	for _, ch := range channels {
		req[ch] = struct{}{}
		wm[ch] = nextSeq - 1
	}
	c := &frameCollector{
		channels:   append([]ChannelID(nil), channels...),
		required:   req,
		frames:     make(map[int64]map[ChannelID]Sample),
		watermark:  wm,
		closedUpTo: nextSeq - 1,
	}
	c.setMaxPending(maxPending)
	return c
}

// setMaxPending 在线调整积压上限（供 UpdateConfig 使用）。
func (c *frameCollector) setMaxPending(n int) {
	if n < 1 {
		n = 1
	}
	c.maxPending.Store(int64(n))
}

// offer 放入一条采样，返回本次关闭的帧（按序号升序，可能为零个或多个）。
// dropped 为 true 表示该样本被确定性丢弃：未知通道、已封闭时刻的迟到/
// 乱序样本，或同一时刻同一通道的重复读数（仅首次接收的读数生效）。
func (c *frameCollector) offer(s Sample) (closed []map[ChannelID]Sample, dropped bool) {
	if _, known := c.required[s.Channel]; !known {
		return nil, true
	}
	if s.Seq <= c.closedUpTo {
		// 已封闭时刻：迟到/重复/乱序样本确定性丢弃。
		return nil, true
	}
	if s.Seq > c.watermark[s.Channel] {
		c.watermark[s.Channel] = s.Seq
	}

	frame := c.frames[s.Seq]
	if frame == nil {
		frame = make(map[ChannelID]Sample, len(c.channels))
		c.frames[s.Seq] = frame
	}
	if _, exists := frame[s.Channel]; exists {
		// 同一时刻同一通道重复上报：只有首次接收的读数生效。
		return nil, true
	}
	frame[s.Channel] = s

	// 正常关闭：最旧待处理帧的全部通道到位（有读数或确认整拍缺数）。
	for {
		oldest := c.oldestPending()
		if oldest < 0 || !c.allAccountedFor(oldest) {
			break
		}
		closed = append(closed, c.pop(oldest))
	}
	// 积压上限：强制关闭最旧帧，缺报通道按整拍缺数降级处理。
	for int64(len(c.frames)) > c.maxPending.Load() {
		oldest := c.oldestPending()
		if oldest < 0 {
			break
		}
		closed = append(closed, c.pop(oldest))
	}
	return closed, false
}

// drain 在输入结束时关闭全部仍缓冲的帧（按序号升序）：
// 不再等待缺报通道，按实际收到的样本降级输出，保证已接受样本全部处理完毕。
func (c *frameCollector) drain() []map[ChannelID]Sample {
	var out []map[ChannelID]Sample
	for {
		oldest := c.oldestPending()
		if oldest < 0 {
			return out
		}
		out = append(out, c.pop(oldest))
	}
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

// pop 取出指定帧并推进封闭水位：该时刻此后对任何读数封闭。
func (c *frameCollector) pop(seq int64) map[ChannelID]Sample {
	frame := c.frames[seq]
	delete(c.frames, seq)
	if seq > c.closedUpTo {
		c.closedUpTo = seq
	}
	return frame
}
