package sensor

// Replay 以确定性方式回放采样序列，复现状态迁移与表决输出。
//
// 回放与在线运行共用同一个 frameCollector 与 pipeline：
// 样本按给定顺序逐条进入组装器，帧关闭规则（水位、积压上限、
// 封闭水位、重复丢弃）与在线路径完全一致，结尾 drain 等价于停机排空。
// 因此相同配置 + 相同采样序列下，在线运行、停机排空与离线回放
// 产生逐条一致的表决结果与状态迁移。
//
// 回放过程不接触墙钟、不启动 goroutine、不访问文件。
func Replay(cfg Config, samples []Sample) ([]VoteResult, error) {
	cfg = cfg.clone()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	p := newPipeline(cfg)
	collector := newFrameCollector(cfg.Channels, 0, cfg.maxPendingFrames())

	var results []VoteResult
	emit := func(frame map[ChannelID]Sample) {
		result, _ := p.tick(frameSeq(frame), frame)
		results = append(results, result)
	}
	for _, s := range samples {
		frames, _ := collector.offer(s)
		for _, frame := range frames {
			emit(frame)
		}
	}
	// 输入结束：与停机排空相同，按实际到达样本关闭全部剩余帧。
	for _, frame := range collector.drain() {
		emit(frame)
	}
	return results, nil
}
