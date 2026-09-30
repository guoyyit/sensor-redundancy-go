package sensor

// history 是每通道有界的近期采样历史。
type history struct {
	capacity int
	samples  map[ChannelID][]Sample
}

func newHistory(capacity int) *history {
	if capacity < 1 {
		capacity = 1
	}
	return &history{
		capacity: capacity,
		samples:  make(map[ChannelID][]Sample),
	}
}

// add 追加一条采样，超出容量时丢弃最旧一条，保证缓冲有界。
func (h *history) add(s Sample) {
	buf := h.samples[s.Channel]
	buf = append(buf, s)
	if len(buf) > h.capacity {
		buf = buf[len(buf)-h.capacity:]
	}
	h.samples[s.Channel] = buf
}

// get 返回该通道近期采样的拷贝（旧 -> 新），调用方修改不影响内部状态。
func (h *history) get(ch ChannelID) []Sample {
	buf := h.samples[ch]
	if len(buf) == 0 {
		return nil
	}
	out := make([]Sample, len(buf))
	copy(out, buf)
	return out
}

// latest 返回该通道最近一条采样。
func (h *history) latest(ch ChannelID) (Sample, bool) {
	buf := h.samples[ch]
	if len(buf) == 0 {
		return Sample{}, false
	}
	return buf[len(buf)-1], true
}

// snapshot 返回全部通道历史的深拷贝，供持久化使用。
func (h *history) snapshot(channels []ChannelID) map[ChannelID][]Sample {
	out := make(map[ChannelID][]Sample, len(channels))
	for _, ch := range channels {
		if buf := h.samples[ch]; len(buf) > 0 {
			cp := make([]Sample, len(buf))
			copy(cp, buf)
			out[ch] = cp
		} else {
			out[ch] = nil
		}
	}
	return out
}

// restore 用持久化快照重建历史，丢弃超出容量的旧样本。
func (h *history) restore(snap map[ChannelID][]Sample) {
	h.samples = make(map[ChannelID][]Sample, len(snap))
	for ch, buf := range snap {
		if len(buf) > h.capacity {
			buf = buf[len(buf)-h.capacity:]
		}
		cp := make([]Sample, len(buf))
		copy(cp, buf)
		h.samples[ch] = cp
	}
}
