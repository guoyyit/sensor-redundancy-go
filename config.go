package sensor

import "fmt"

// Config 是表决与健康评估的全部可调参数。
// 数值量纲与被测物理量一致，阈值均为非负绝对值。
type Config struct {
	// Channels 参与表决的通道集合，顺序决定确定性输出。
	Channels []ChannelID
	// DriftThreshold 与表决参考值的最大允许偏差，超出记一次失配。
	DriftThreshold float64
	// JumpThreshold 相邻两次读数间允许的最大跳变量。
	JumpThreshold float64
	// StuckEpsilon 判定读数不变的容差。
	StuckEpsilon float64
	// StuckWindow 判定卡死所需的连续不变样本数。
	StuckWindow int
	// SuspectAfter 连续失配达到该次数进入疑似。
	SuspectAfter int
	// FaultAfter 连续失配达到该次数隔离通道。
	FaultAfter int
	// RecoverAfter 已隔离通道连续匹配达到该次数重新纳入。
	RecoverAfter int
	// MinHealthy 允许输出表决值的最少健康/可用通道数。
	MinHealthy int
	// HistorySize 每通道保留的近期采样历史长度（有界）。
	HistorySize int
	// MaxPendingFrames 输入侧允许积压的未闭合采样帧上限。
	// 某通道长时间缺报时，积压达到上限即按“已确定整拍缺数”
	// 强制关闭最旧帧（以实际到位的通道集合降级输出），保证
	// 内存有界且推进规则确定（只取决于样本序列，不依赖墙钟）。
	// 0 表示使用默认值 DefaultMaxPendingFrames。
	MaxPendingFrames int
}

// DefaultMaxPendingFrames 是 MaxPendingFrames 的默认值。
// 取值明显大于输入管道容量（len(Channels)*4）：健康生产者因调度
// 倾斜产生的正常积压在管道容量量级，不会触碰该上限；只有某通道
// 持续缺报（其余通道不断推进）时积压才会持续增长并触发强制关闭。
const DefaultMaxPendingFrames = 64

// DefaultConfig 返回给定通道集合下的默认配置。
func DefaultConfig(channels []ChannelID) Config {
	return Config{
		Channels:         append([]ChannelID(nil), channels...),
		DriftThreshold:   1.0,
		JumpThreshold:    5.0,
		StuckEpsilon:     1e-9,
		StuckWindow:      5,
		SuspectAfter:     2,
		FaultAfter:       3,
		RecoverAfter:     3,
		MinHealthy:       2,
		HistorySize:      16,
		MaxPendingFrames: DefaultMaxPendingFrames,
	}
}

// Validate 校验配置合法性。
func (c Config) Validate() error {
	if len(c.Channels) == 0 {
		return fmt.Errorf("sensor: at least one channel required")
	}
	seen := make(map[ChannelID]struct{}, len(c.Channels))
	for _, ch := range c.Channels {
		if ch == "" {
			return fmt.Errorf("sensor: empty channel id")
		}
		if _, dup := seen[ch]; dup {
			return fmt.Errorf("sensor: duplicate channel %q", ch)
		}
		seen[ch] = struct{}{}
	}
	if c.DriftThreshold < 0 {
		return fmt.Errorf("sensor: DriftThreshold must be non-negative")
	}
	if c.JumpThreshold < 0 {
		return fmt.Errorf("sensor: JumpThreshold must be non-negative")
	}
	if c.StuckEpsilon < 0 {
		return fmt.Errorf("sensor: StuckEpsilon must be non-negative")
	}
	if c.StuckWindow < 2 {
		return fmt.Errorf("sensor: StuckWindow must be >= 2")
	}
	if c.SuspectAfter < 1 {
		return fmt.Errorf("sensor: SuspectAfter must be >= 1")
	}
	if c.FaultAfter < c.SuspectAfter {
		return fmt.Errorf("sensor: FaultAfter must be >= SuspectAfter")
	}
	if c.RecoverAfter < 1 {
		return fmt.Errorf("sensor: RecoverAfter must be >= 1")
	}
	if c.MinHealthy < 1 {
		return fmt.Errorf("sensor: MinHealthy must be >= 1")
	}
	if c.HistorySize < c.StuckWindow+1 {
		return fmt.Errorf("sensor: HistorySize must be >= StuckWindow+1")
	}
	if c.MaxPendingFrames < 0 {
		return fmt.Errorf("sensor: MaxPendingFrames must be non-negative")
	}
	return nil
}

// maxPendingFrames 返回生效的待处理帧上限；0 表示默认。
func (c Config) maxPendingFrames() int {
	if c.MaxPendingFrames <= 0 {
		return DefaultMaxPendingFrames
	}
	return c.MaxPendingFrames
}

// clone 返回配置的深拷贝，避免在线调整时共享底层切片。
func (c Config) clone() Config {
	c.Channels = append([]ChannelID(nil), c.Channels...)
	return c
}
