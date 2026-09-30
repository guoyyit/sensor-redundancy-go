package sensor

import "fmt"

// HealthState 是通道健康状态机的状态。
type HealthState int

const (
	// StateHealthy 健康，参与表决。
	StateHealthy HealthState = iota
	// StateSuspected 疑似，仍参与表决但被重点观察。
	StateSuspected
	// StateFaulty 故障，已隔离，不参与表决。
	StateFaulty
	// StateRecovering 恢复中，正在用连续匹配证明自身健康。
	StateRecovering
)

// String 返回状态的可读名称。
func (s HealthState) String() string {
	switch s {
	case StateHealthy:
		return "healthy"
	case StateSuspected:
		return "suspected"
	case StateFaulty:
		return "faulty"
	case StateRecovering:
		return "recovering"
	default:
		return fmt.Sprintf("state(%d)", int(s))
	}
}

// ChannelStatus 是单条通道的健康评估状态。
type ChannelStatus struct {
	State          HealthState
	MismatchStreak int
	MatchStreak    int
}

// HealthTransition 记录一次状态迁移，用于可解释输出与回放复现。
type HealthTransition struct {
	From   HealthState
	To     HealthState
	Reason string
}

// newChannelStatus 返回初始健康状态。
func newChannelStatus() ChannelStatus {
	return ChannelStatus{State: StateHealthy}
}

// available 表示该状态的通道是否参与当前表决。
// 健康与疑似通道参与；故障与恢复中通道被隔离。
func (s HealthState) available() bool {
	return s == StateHealthy || s == StateSuspected
}

// observe 按“连续失配下行、连续匹配上行”推进一个采样拍。
// matched 为该通道本拍读数与表决参考是否一致。
//
// 迁移规则：
//   - healthy：连续失配达 SuspectAfter 进入 suspected，达 FaultAfter 隔离为 faulty；
//     任一匹配立即清零失配计数（瞬态抖动不会累积到隔离）。
//   - suspected：继续失配达 FaultAfter 隔离；任一匹配清零并回到 healthy。
//   - faulty：出现一次匹配进入 recovering（恢复中仍被隔离）。
//   - recovering：连续匹配达 RecoverAfter 重新纳入为 healthy；
//     任一失配立即退回 faulty 并清零匹配计数。
func (st *ChannelStatus) observe(matched bool, cfg Config) HealthTransition {
	from := st.State
	reason := ""

	switch st.State {
	case StateHealthy, StateSuspected:
		if matched {
			st.MismatchStreak = 0
			st.MatchStreak = 0
			if st.State == StateSuspected {
				st.State = StateHealthy
				reason = "match cleared suspicion"
			}
		} else {
			st.MismatchStreak++
			st.MatchStreak = 0
			switch {
			case st.MismatchStreak >= cfg.FaultAfter:
				st.State = StateFaulty
				reason = fmt.Sprintf("mismatch streak %d >= fault-after %d", st.MismatchStreak, cfg.FaultAfter)
			case st.MismatchStreak >= cfg.SuspectAfter:
				st.State = StateSuspected
				reason = fmt.Sprintf("mismatch streak %d >= suspect-after %d", st.MismatchStreak, cfg.SuspectAfter)
			}
		}
	case StateFaulty:
		if matched {
			st.MatchStreak = 1
			st.MismatchStreak = 0
			st.State = StateRecovering
			reason = "first match while isolated, entering recovery"
		}
	case StateRecovering:
		if matched {
			st.MatchStreak++
			if st.MatchStreak >= cfg.RecoverAfter {
				st.State = StateHealthy
				st.MatchStreak = 0
				reason = fmt.Sprintf("match streak %d >= recover-after %d", st.MatchStreak, cfg.RecoverAfter)
			}
		} else {
			st.State = StateFaulty
			st.MatchStreak = 0
			reason = "mismatch during recovery, re-isolated"
		}
	}

	if st.State == from {
		return HealthTransition{From: from, To: from}
	}
	return HealthTransition{From: from, To: st.State, Reason: reason}
}
