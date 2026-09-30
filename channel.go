package redundancy

// channelState holds the mutable per-channel state.
type channelState struct {
	id          ChannelID
	health      HealthState
	mismatchRun int
	matchRun    int
	history     []Sample
	pending     []Sample
}

func newChannelState(id ChannelID) *channelState {
	return &channelState{id: id, health: HealthHealthy}
}

// participates reports whether the channel currently contributes to voting.
func (c *channelState) participates() bool {
	return c.health == HealthHealthy || c.health == HealthSuspect
}

// record folds one round's match/mismatch verdict into the consecutive
// counters and migrates the health state. Transient jitter cannot isolate
// a channel: only MismatchLimit consecutive mismatches do, and a single
// match clears the run.
func (c *channelState) record(matched bool, cfg Config) {
	if matched {
		c.matchRun++
		c.mismatchRun = 0
		switch c.health {
		case HealthSuspect:
			c.health = HealthHealthy
		case HealthFaulty:
			c.health = HealthRecovering
		case HealthRecovering:
			if c.matchRun >= cfg.RecoverWindow {
				c.health = HealthHealthy
				c.matchRun = 0
			}
		}
		return
	}

	c.mismatchRun++
	c.matchRun = 0
	switch c.health {
	case HealthHealthy:
		c.health = HealthSuspect
	case HealthSuspect:
		if c.mismatchRun >= cfg.MismatchLimit {
			c.health = HealthFaulty
		}
	case HealthRecovering:
		// A relapse during the recovery window re-isolates the channel.
		c.health = HealthFaulty
		c.mismatchRun = cfg.MismatchLimit
	}
}

// pushHistory appends a sample to the bounded recent-history ring.
func (c *channelState) pushHistory(s Sample, size int) {
	if size <= 0 {
		return
	}
	c.history = append(c.history, s)
	if len(c.history) > size {
		c.history = append(c.history[:0], c.history[len(c.history)-size:]...)
	}
}

func (c *channelState) status() ChannelStatus {
	st := ChannelStatus{
		ID:          c.id,
		Health:      c.health,
		MismatchRun: c.mismatchRun,
		MatchRun:    c.matchRun,
	}
	if n := len(c.history); n > 0 {
		st.Last = c.history[n-1]
	}
	return st
}
