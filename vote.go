package sensor

import (
	"math"
	"sort"
)

// VoteStatus 是表决输出的可信级别。
type VoteStatus string

const (
	// StatusOK 全通道或多数通道一致。
	StatusOK VoteStatus = "ok"
	// StatusDegraded 可用通道减少，按降级规则输出。
	StatusDegraded VoteStatus = "degraded"
	// StatusFailed 可用通道不足，输出为明确失效状态。
	StatusFailed VoteStatus = "failed"
)

// VoteResult 是一个采样时刻的表决结果。
type VoteResult struct {
	Seq          int64
	Value        float64
	Status       VoteStatus
	Contributors []ChannelID
	Reason       string
}

// voter 按冗余度与可用通道集合执行表决与降级。
type voter struct {
	cfg Config
}

func newVoter(cfg Config) *voter {
	return &voter{cfg: cfg}
}

// vote 对一帧读数（仅含可用通道）进行表决。
//
// 降级后的既定规则：
//   - 0 个可用，或双余度下仅剩 1/2 个但互相不一致：failed，输出 NaN（非错误数值）；
//   - 2 个可用且一致：均值，degraded/ok 取决于是否为完整余度；
//   - 3 个可用：中位数；其中两两一致时取一致对的均值，故障通道无法拖偏输出；
//   - 4 个可用：取最小团簇（直径不超过 DriftThreshold）内成员的均值；
//     2:2 分裂时按配置通道顺序确定地选择先出现的团簇；3:1 时天然由多数团簇胜出。
//
// 单通道仅在 MinHealthy <= 1 时允许作为 degraded 输出。
func (v *voter) vote(seq int64, frame map[ChannelID]float64, configured int) VoteResult {
	if len(frame) == 0 {
		return failed(seq, "no available channels")
	}

	ordered := make([]ChannelID, 0, len(frame))
	for _, ch := range v.cfg.Channels {
		if _, ok := frame[ch]; ok {
			ordered = append(ordered, ch)
		}
	}

	n := len(ordered)
	switch {
	case n >= 3:
		return v.voteCluster(seq, ordered, frame, configured)
	case n == 2:
		return v.voteTwo(seq, ordered, frame, configured)
	default:
		if v.cfg.MinHealthy <= 1 {
			ch := ordered[0]
			return VoteResult{
				Seq:          seq,
				Value:        frame[ch],
				Status:       StatusDegraded,
				Contributors: []ChannelID{ch},
				Reason:       "single available channel",
			}
		}
		return failed(seq, "only one channel available below MinHealthy")
	}
}

// voteTwo 处理恰好两个可用通道：一致取均值，分歧即无法仲裁，判失效。
func (v *voter) voteTwo(seq int64, ordered []ChannelID, frame map[ChannelID]float64, configured int) VoteResult {
	a, b := ordered[0], ordered[1]
	va, vb := frame[a], frame[b]
	if math.Abs(va-vb) <= v.cfg.DriftThreshold {
		return VoteResult{
			Seq:          seq,
			Value:        (va + vb) / 2,
			Status:       statusFor(2, configured),
			Contributors: []ChannelID{a, b},
			Reason:       "two channels agree within drift threshold",
		}
	}
	return failed(seq, "two channels disagree beyond drift threshold")
}

// voteCluster 处理三个及以上可用通道：选择“最紧、最大”的团簇。
func (v *voter) voteCluster(seq int64, ordered []ChannelID, frame map[ChannelID]float64, configured int) VoteResult {
	values := make([]float64, len(ordered))
	for i, ch := range ordered {
		values[i] = frame[ch]
	}

	members := v.tightestCluster(ordered, values)
	if len(members) < 2 {
		return failed(seq, "no agreeing channel pair within drift threshold")
	}

	var sum float64
	contributors := make([]ChannelID, 0, len(members))
	for _, idx := range members {
		sum += values[idx]
		contributors = append(contributors, ordered[idx])
	}
	mean := sum / float64(len(members))

	reason := "majority/tightest cluster"
	if len(members) < len(ordered) {
		reason = "majority/tightest cluster after excluding outlier(s)"
	}
	return VoteResult{
		Seq:          seq,
		Value:        mean,
		Status:       statusFor(len(ordered), configured),
		Contributors: contributors,
		Reason:       reason,
	}
}

// tightestCluster 返回满足“团簇直径 <= DriftThreshold”的最优成员下标集合。
// 排序后在滑动窗口内选择：优先成员数最多，其次直径最小；平局由配置顺序打破，
// 从而保证相同输入下输出逐条一致。
func (v *voter) tightestCluster(ordered []ChannelID, values []float64) []int {
	type point struct {
		idx int
		val float64
	}
	points := make([]point, len(ordered))
	for i := range ordered {
		points[i] = point{idx: i, val: values[i]}
	}
	// idx 作为次级键，消除排序平局。
	sort.SliceStable(points, func(i, j int) bool {
		if points[i].val != points[j].val {
			return points[i].val < points[j].val
		}
		return points[i].idx < points[j].idx
	})

	best := []int{points[0].idx}
	bestSpread := 0.0
	for l := 0; l < len(points); l++ {
		for r := l; r < len(points); r++ {
			spread := points[r].val - points[l].val
			if spread > v.cfg.DriftThreshold {
				break
			}
			size := r - l + 1
			if size < len(best) {
				continue
			}
			members := make([]int, 0, size)
			for k := l; k <= r; k++ {
				members = append(members, points[k].idx)
			}
			sort.Ints(members)
			if !v.betterCluster(members, size, spread, best, bestSpread) {
				continue
			}
			best = members
			bestSpread = spread
		}
	}
	return best
}

// betterCluster 判定候选团簇是否优于当前最优：
// 成员更多优先；数量相同则直径更小；再相同则团簇中最早的配置通道
// （最小下标）更靠前，保证 2:2 分裂时的选择确定且可解释。
func (v *voter) betterCluster(members []int, size int, spread float64, best []int, bestSpread float64) bool {
	switch {
	case size > len(best):
		return true
	case size < len(best):
		return false
	case spread < bestSpread:
		return true
	case spread > bestSpread:
		return false
	default:
		return members[0] < best[0]
	}
}

// statusFor 根据参与通道数与配置余度判定可信级别。
func statusFor(available, configured int) VoteStatus {
	if available < configured {
		return StatusDegraded
	}
	return StatusOK
}

// failed 构造明确的失效结果；NaN 表示“无数值”，不会被当作错误读数。
func failed(seq int64, reason string) VoteResult {
	return VoteResult{
		Seq:          seq,
		Value:        math.NaN(),
		Status:       StatusFailed,
		Contributors: nil,
		Reason:       reason,
	}
}

// fallbackReference 在表决失效时寻找“仅供内部健康判定”的多数共识。
// 与 vote 的团簇规则一致，但放宽为：只要存在至少两个互相一致的通道，
// 就取成员最多/最紧团簇的均值作为参考；没有任何一致对（全部分歧）时
// 返回 ok=false，表示这一拍确实没有可信真相，健康状态保持不变。
func fallbackReference(cfg Config, frame map[ChannelID]float64) (float64, bool) {
	if len(frame) < 2 {
		return 0, false
	}
	ordered := make([]ChannelID, 0, len(frame))
	for _, ch := range cfg.Channels {
		if _, ok := frame[ch]; ok {
			ordered = append(ordered, ch)
		}
	}
	values := make([]float64, len(ordered))
	for i, ch := range ordered {
		values[i] = frame[ch]
	}

	type point struct {
		idx int
		val float64
	}
	points := make([]point, len(ordered))
	for i := range ordered {
		points[i] = point{idx: i, val: values[i]}
	}
	sort.SliceStable(points, func(i, j int) bool {
		if points[i].val != points[j].val {
			return points[i].val < points[j].val
		}
		return points[i].idx < points[j].idx
	})

	var (
		bestL, bestR = -1, -1
		bestSize     = 1
		bestSpread   = math.Inf(1)
		bestFirstIdx = 0
	)
	for l := 0; l < len(points); l++ {
		for r := l; r < len(points); r++ {
			spread := points[r].val - points[l].val
			if spread > cfg.DriftThreshold {
				break
			}
			size := r - l + 1
			better := size > bestSize ||
				(size == bestSize && spread < bestSpread) ||
				(size == bestSize && spread == bestSpread && points[l].idx < bestFirstIdx)
			if better {
				bestL, bestR = l, r
				bestSize = size
				bestSpread = spread
				bestFirstIdx = points[l].idx
			}
		}
	}
	if bestSize < 2 {
		return 0, false
	}
	sum := 0.0
	for k := bestL; k <= bestR; k++ {
		sum += points[k].val
	}
	return sum / float64(bestSize), true
}
