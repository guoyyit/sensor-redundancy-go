package sensor

import "sort"

// Replay 以确定性方式回放采样序列，复现状态迁移与表决输出。
//
// 回放过程不接触墙钟、不启动 goroutine、不访问文件：
// 相同配置 + 相同采样序列必然产生逐条一致的结果序列。
// 帧按 Seq 分组，Seq 升序处理；同一帧内按配置通道顺序处理健康迁移，
// 以消除遍历顺序的不确定性。
func Replay(cfg Config, samples []Sample) ([]VoteResult, error) {
	cfg = cfg.clone()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	frames := groupFrames(cfg.Channels, samples)
	seqs := make([]int64, 0, len(frames))
	for seq := range frames {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	p := newPipeline(cfg)
	results := make([]VoteResult, 0, len(seqs))
	for _, seq := range seqs {
		result, _ := p.tick(seq, frames[seq])
		results = append(results, result)
	}
	return results, nil
}

// groupFrames 把扁平采样序列按 Seq 分组。
// 同一通道在同一 Seq 出现多次时保留首次接收的读数，
// 与在线收集器的“首达生效、重复丢弃”语义一致。
func groupFrames(channels []ChannelID, samples []Sample) map[int64]map[ChannelID]Sample {
	frames := make(map[int64]map[ChannelID]Sample)
	known := make(map[ChannelID]struct{}, len(channels))
	for _, ch := range channels {
		known[ch] = struct{}{}
	}
	for _, s := range samples {
		if _, ok := known[s.Channel]; !ok {
			continue
		}
		frame := frames[s.Seq]
		if frame == nil {
			frame = make(map[ChannelID]Sample)
			frames[s.Seq] = frame
		}
		if _, exists := frame[s.Channel]; !exists {
			frame[s.Channel] = s
		}
	}
	return frames
}
