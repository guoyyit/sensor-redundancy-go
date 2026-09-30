package redundancy

import (
	"reflect"
	"testing"
)

func samplesOf(values ...float64) []Sample {
	out := make([]Sample, len(values))
	for i, v := range values {
		out[i] = Sample{Channel: ChannelID(i), Value: v}
	}
	return out
}

func TestVoteAllAgree(t *testing.T) {
	got := vote(samplesOf(10.0, 10.4, 9.8), 1.0, 3, 1)
	if !got.Valid || got.Degraded {
		t.Fatalf("expected valid non-degraded vote, got %+v", got)
	}
	if want := 30.2 / 3; got.Value < want-1e-9 || got.Value > want+1e-9 {
		t.Fatalf("value = %v, want mean %v", got.Value, want)
	}
	if want := []ChannelID{0, 1, 2}; !reflect.DeepEqual(got.Contributors, want) {
		t.Fatalf("contributors = %v, want %v", got.Contributors, want)
	}
}

func TestVoteTripleWithDrifter(t *testing.T) {
	got := vote(samplesOf(10.0, 10.2, 15.0), 1.0, 3, 1)
	if !got.Valid || !got.Degraded {
		t.Fatalf("expected valid degraded vote, got %+v", got)
	}
	if got.Value != 10.1 {
		t.Fatalf("value = %v, want 10.1 (drifter excluded)", got.Value)
	}
	if want := []ChannelID{0, 1}; !reflect.DeepEqual(got.Contributors, want) {
		t.Fatalf("contributors = %v, want %v", got.Contributors, want)
	}
}

func TestVoteQuadWithOutlier(t *testing.T) {
	got := vote(samplesOf(20.0, 20.3, 19.9, 99.0), 1.0, 4, 1)
	if !got.Valid || !got.Degraded {
		t.Fatalf("expected valid degraded vote, got %+v", got)
	}
	if want := []ChannelID{0, 1, 2}; !reflect.DeepEqual(got.Contributors, want) {
		t.Fatalf("contributors = %v, want %v", got.Contributors, want)
	}
	if got.Value < 19.9 || got.Value > 20.3 {
		t.Fatalf("value %v dragged by outlier", got.Value)
	}
}

func TestVoteDualAgree(t *testing.T) {
	got := vote(samplesOf(5.0, 5.5), 1.0, 2, 1)
	if !got.Valid || got.Degraded {
		t.Fatalf("expected valid non-degraded vote, got %+v", got)
	}
	if got.Value != 5.25 {
		t.Fatalf("value = %v, want 5.25", got.Value)
	}
}

func TestVoteDualDisagree(t *testing.T) {
	got := vote(samplesOf(5.0, 9.0), 1.0, 2, 1)
	if got.Valid {
		t.Fatalf("disagreeing dual inputs must not produce a value, got %+v", got)
	}
}

func TestVoteSingleParticipant(t *testing.T) {
	got := vote(samplesOf(7.5), 1.0, 3, 1)
	if !got.Valid || !got.Degraded {
		t.Fatalf("single remaining channel should yield degraded output, got %+v", got)
	}
	if got.Value != 7.5 {
		t.Fatalf("value = %v, want 7.5", got.Value)
	}
	if got := vote(samplesOf(7.5), 1.0, 3, 2); got.Valid {
		t.Fatalf("minVoters=2 must reject a single participant, got %+v", got)
	}
}

func TestVoteNoParticipants(t *testing.T) {
	if got := vote(nil, 1.0, 3, 1); got.Valid {
		t.Fatalf("empty input must be invalid, got %+v", got)
	}
}

func TestVoteTieBreakDeterministic(t *testing.T) {
	// Two equally sized windows with equal span: the lower one wins.
	got := vote(samplesOf(0, 1, 2), 1.0, 3, 1)
	if want := []ChannelID{0, 1}; !reflect.DeepEqual(got.Contributors, want) {
		t.Fatalf("contributors = %v, want %v", got.Contributors, want)
	}
	// Equal size, different span: the tighter cluster wins.
	got = vote(samplesOf(0, 0.5, 3, 3.9), 1.0, 4, 1)
	if want := []ChannelID{0, 1}; !reflect.DeepEqual(got.Contributors, want) {
		t.Fatalf("contributors = %v, want %v (tighter pair)", got.Contributors, want)
	}
}

func TestVoteQuadSplitPairs(t *testing.T) {
	got := vote(samplesOf(1.0, 1.2, 5.0, 5.2), 1.0, 4, 1)
	if !got.Valid || !got.Degraded {
		t.Fatalf("split quad should degrade to one pair, got %+v", got)
	}
	if want := []ChannelID{0, 1}; !reflect.DeepEqual(got.Contributors, want) {
		t.Fatalf("contributors = %v, want %v", got.Contributors, want)
	}
}
