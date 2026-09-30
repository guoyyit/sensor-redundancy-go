// Package redundancy implements voting and fault isolation for redundant
// sensor channels measuring the same physical quantity.
package redundancy

import "fmt"

// HealthState describes the lifecycle of a single sensor channel.
type HealthState int

const (
	// HealthSuspect means the channel has recent consecutive mismatches
	// below the isolation limit. It still participates in voting.
	HealthSuspect HealthState = iota
	// HealthFaulty means the channel is isolated and excluded from voting.
	HealthFaulty
	// HealthRecovering means a previously isolated channel is accumulating
	// consecutive matches before being re-included.
	HealthRecovering
	// HealthHealthy means the channel fully participates in voting.
	HealthHealthy
)

func (s HealthState) String() string {
	switch s {
	case HealthSuspect:
		return "suspect"
	case HealthFaulty:
		return "faulty"
	case HealthRecovering:
		return "recovering"
	case HealthHealthy:
		return "healthy"
	default:
		return "unknown"
	}
}

// ChannelID identifies one redundant sensor channel.
type ChannelID int

// Sample is one measurement from one channel. Seq is a logical sequence
// number shared across channels so samples from the same instant align.
type Sample struct {
	Channel ChannelID
	Seq     uint64
	Value   float64
}

// VoteResult is the voted output for one sampling round.
type VoteResult struct {
	Value        float64
	Valid        bool
	Degraded     bool
	Contributors []ChannelID
}

// Config holds the static configuration of a Monitor.
type Config struct {
	Redundancy    int
	Threshold     float64
	MismatchLimit int
	RecoverWindow int
	MinVoters     int
	HistorySize   int
}

func (c Config) validate() error {
	if c.Redundancy < 2 || c.Redundancy > 4 {
		return fmt.Errorf("redundancy: Redundancy must be 2, 3 or 4, got %d", c.Redundancy)
	}
	if c.Threshold <= 0 {
		return fmt.Errorf("redundancy: Threshold must be positive, got %v", c.Threshold)
	}
	if c.MismatchLimit < 1 {
		return fmt.Errorf("redundancy: MismatchLimit must be >= 1, got %d", c.MismatchLimit)
	}
	if c.RecoverWindow < 1 {
		return fmt.Errorf("redundancy: RecoverWindow must be >= 1, got %d", c.RecoverWindow)
	}
	if c.MinVoters < 1 || c.MinVoters > c.Redundancy {
		return fmt.Errorf("redundancy: MinVoters must be in [1,%d], got %d", c.Redundancy, c.MinVoters)
	}
	if c.HistorySize < 0 {
		return fmt.Errorf("redundancy: HistorySize must be >= 0, got %d", c.HistorySize)
	}
	return nil
}

func (c Config) withDefaults() Config {
	if c.MinVoters == 0 {
		c.MinVoters = 1
	}
	return c
}

// ChannelStatus is a consistent read-only view of one channel.
type ChannelStatus struct {
	ID          ChannelID
	Health      HealthState
	MismatchRun int
	MatchRun    int
	Last        Sample
}

// Snapshot is a consistent view of the whole system for one round.
type Snapshot struct {
	Seq      uint64
	Vote     VoteResult
	Channels []ChannelStatus
}
