package sensor

import "math"

// FaultKind 描述一次检测到的故障类型。
type FaultKind int

const (
	FaultNone FaultKind = iota
	FaultDrift
	FaultJump
	FaultStuck
)

// String 返回故障类型的可读名称。
func (k FaultKind) String() string {
	switch k {
	case FaultDrift:
		return "drift"
	case FaultJump:
		return "jump"
	case FaultStuck:
		return "stuck"
	default:
		return "none"
	}
}

// Detection 是单通道单拍的检测结论。
// Matched 表示读数与表决参考一致，是健康状态迁移的依据。
type Detection struct {
	Kind    FaultKind
	Matched bool
}

// detector 依据有界历史对单通道读数做故障检测。
type detector struct {
	cfg Config
}

func newDetector(cfg Config) *detector {
	return &detector{cfg: cfg}
}

// check 判断当前读数相对参考值是否构成故障。
// prior 为该通道此前的采样（旧 -> 新），不含当前读数。
//
// 分类规则（仅在与参考失配时才报故障，避免稳态真实信号被误判卡死）：
//   - 读数与参考偏差不超过 DriftThreshold：一致；
//   - 否则若最近 StuckWindow 个历史读数与当前值之差均不超过 StuckEpsilon：卡死；
//   - 否则若相对上一读数的跳变超过 JumpThreshold：跳变；
//   - 其余失配：漂移。
func (d *detector) check(cur Sample, ref float64, prior []Sample) Detection {
	if math.Abs(cur.Value-ref) <= d.cfg.DriftThreshold {
		return Detection{Kind: FaultNone, Matched: true}
	}

	if d.isStuck(cur.Value, prior) {
		return Detection{Kind: FaultStuck, Matched: false}
	}
	if len(prior) > 0 {
		prev := prior[len(prior)-1].Value
		if math.Abs(cur.Value-prev) > d.cfg.JumpThreshold {
			return Detection{Kind: FaultJump, Matched: false}
		}
	}
	return Detection{Kind: FaultDrift, Matched: false}
}

// isStuck 判断当前读数是否已冻结满 StuckWindow 个历史拍。
func (d *detector) isStuck(cur float64, prior []Sample) bool {
	if d.cfg.StuckWindow <= 0 || len(prior) < d.cfg.StuckWindow {
		return false
	}
	tail := prior[len(prior)-d.cfg.StuckWindow:]
	for _, s := range tail {
		if math.Abs(s.Value-cur) > d.cfg.StuckEpsilon {
			return false
		}
	}
	return true
}
