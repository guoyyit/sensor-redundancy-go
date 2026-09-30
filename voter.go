package redundancy

import "sort"

// vote computes the voted output from the participating channels' samples
// for one round. expected is the configured redundancy, used to flag
// degraded operation. minVoters is the smallest acceptable voting set.
//
// The rule is deterministic: among the participants, find the largest set
// of channels whose values span at most threshold; ties are broken by
// smaller span, then by lower channel ids. The output is the mean of that
// set. With no agreeing pair, a single remaining participant may still
// yield a degraded output when minVoters allows it; otherwise the result
// is invalid rather than a misleading number.
func vote(samples []Sample, threshold float64, expected, minVoters int) VoteResult {
	if len(samples) == 0 {
		return VoteResult{}
	}

	sorted := make([]Sample, len(samples))
	copy(sorted, samples)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Value != sorted[j].Value {
			return sorted[i].Value < sorted[j].Value
		}
		return sorted[i].Channel < sorted[j].Channel
	})

	bestLo, bestHi := 0, 0 // inclusive window into sorted
	for lo := 0; lo < len(sorted); lo++ {
		hi := lo
		for hi+1 < len(sorted) && sorted[hi+1].Value-sorted[lo].Value <= threshold {
			hi++
		}
		if betterWindow(sorted, lo, hi, bestLo, bestHi) {
			bestLo, bestHi = lo, hi
		}
	}
	win := sorted[bestLo : bestHi+1]

	if len(win) == 1 && (len(samples) > 1 || minVoters > 1) {
		// No agreeing pair among several participants, or a single
		// participant below the minimum voting set: refuse to output
		// a number we cannot justify.
		return VoteResult{}
	}

	var sum float64
	contributors := make([]ChannelID, len(win))
	for i, s := range win {
		sum += s.Value
		contributors[i] = s.Channel
	}
	sort.Slice(contributors, func(i, j int) bool { return contributors[i] < contributors[j] })

	return VoteResult{
		Value:        sum / float64(len(win)),
		Valid:        true,
		Degraded:     len(win) < expected,
		Contributors: contributors,
	}
}

// betterWindow reports whether the candidate window [lo,hi] beats the
// current best [bestLo,bestHi]: larger size wins, then smaller span.
func betterWindow(sorted []Sample, lo, hi, bestLo, bestHi int) bool {
	size, bestSize := hi-lo+1, bestHi-bestLo+1
	if size != bestSize {
		return size > bestSize
	}
	span := sorted[hi].Value - sorted[lo].Value
	bestSpan := sorted[bestHi].Value - sorted[bestLo].Value
	return span < bestSpan
}
