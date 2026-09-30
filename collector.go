package sensor

// frameCollector 把并发到达的单通道采样按采样序号组装成完整帧。
//
// 帧关闭规则（确定性，不依赖到达时延或墙钟）：
//   - 对最旧待处理帧，某通道在“帧内已有该拍读数”或“水位已严格越过
//     该序号（确认整拍缺数）”时才算到位；所有通道到位后关闭。
//     水位仅等于该序号却尚未见到读数时继续等待——该读数可能已在管道中，
//     避免把正在并发投递的正常样本误判为缺失；
//   - 输入结束 flush 时不再等待，按实际收到的样本关闭（真实缺失降级）；
//   - 低于已关闭水位的迟到读数一律丢弃，绝不混入已关闭的帧。
type frameCollector struct {
	channels  []ChannelID
	required  map[ChannelID]struct{}
	frames    map[int64]map[ChannelID]Sample
	watermark map[ChannelID]int64
}

func newFrameCollector(channels []ChannelID, startSeq int64) *frameCollector {
	req := make(map[ChannelID]struct{}, len(channels))
	for _, ch := range channels {
		req[ch] = struct{}{}
	}
	return &frameCollector{
		channels: append([]ChannelID(nil), channels...),
		required: req,
		frames:   make(map[int64]map[ChannelID]Sample),
		watermark: func() map[ChannelID]int64 {
			m := make(map[ChannelID]int64, len(channels))
			for _, ch := range channels {
				m[ch] = startSeq - 1
			}
			return m
		}(),
	}
}

// offer 放入一条采样；若有关闭的帧则返回该帧（每拍最多返回一次）。
func (c *frameCollector) offer(s Sample) map[ChannelID]Sample {
	if _, known := c.required[s.Channel]; !known {
		return nil
	}
	oldest := c.oldestPending()
	// 水位始终推进到该通道见过的最大序号（乱序到达也能关闭旧帧）。
	if s.Seq > c.watermark[s.Channel] {
		c.watermark[s.Channel] = s.Seq
	}

	// 低于最旧待处理帧的读数是迟到样本：仅推进水位，不入帧。
	if oldest < 0 || s.Seq >= oldest {
		frame := c.frames[s.Seq]
		if frame == nil {
			frame = make(map[ChannelID]Sample, len(c.channels))
			c.frames[s.Seq] = frame
		}
		if _, exists := frame[s.Channel]; !exists {
			frame[s.Channel] = s
		}
	}

	// 所有通道对该帧“到位”（有读数或确认越过）时关闭。
	oldest = c.oldestPending()
	if oldest >= 0 && c.allAccountedFor(oldest) {
		return c.pop(oldest)
	}
	return nil
}

// flush 在输入结束时关闭仍缓冲的最旧帧。
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

// pop 取出指定帧并推进 oldest 到下一个仍缓冲的序号。
func (c *frameCollector) pop(seq int64) map[ChannelID]Sample {
	frame := c.frames[seq]
	delete(c.frames, seq)
	return frame
}
