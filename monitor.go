package redundancy

import (
	"errors"
	"sync"
)

// maxPending bounds each channel's queue of unprocessed samples.
const maxPending = 1024

// Monitor ingests concurrent samples, votes per round, tracks channel
// health and persists state for crash recovery.
//
// A round completes when every configured channel has delivered a sample
// for the same sequence number, so readers always observe a complete,
// self-consistent state for one sampling instant. All state mutation
// happens in the single ingest goroutine; results are published under a
// read-write lock.
type Monitor struct {
	cfg      Config
	channels []*channelState
	store    Store

	inbox     chan Sample
	configCh  chan ConfigUpdate
	quit      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	mu         sync.RWMutex
	latest     Snapshot
	persistErr error

	nextSeq uint64
	onRound func(Snapshot)

	lastValid    float64
	hasLastValid bool
}

// NewMonitor creates a Monitor, restoring persisted state when available.
func NewMonitor(cfg Config, store Store) (*Monitor, error) {
	m, err := newMonitor(cfg, store)
	if err != nil {
		return nil, err
	}
	m.wg.Add(1)
	go m.run()
	return m, nil
}

// newMonitor builds a Monitor without starting the ingest goroutine, so
// tests and replay can drive it synchronously.
func newMonitor(cfg Config, store Store) (*Monitor, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	m := &Monitor{
		cfg:      cfg,
		store:    store,
		inbox:    make(chan Sample, maxPending),
		configCh: make(chan ConfigUpdate, 16),
		quit:     make(chan struct{}),
	}
	for i := 0; i < cfg.Redundancy; i++ {
		m.channels = append(m.channels, newChannelState(ChannelID(i)))
	}
	if store != nil {
		if st, err := store.Load(); err == nil {
			m.restore(st)
		} else if !errors.Is(err, ErrNoState) {
			return nil, err
		}
	}
	return m, nil
}

// OpenPipe returns the write end of the in-memory pipe for one channel.
// It returns nil for an out-of-range channel id.
func (m *Monitor) OpenPipe(id ChannelID) *Pipe {
	if id < 0 || int(id) >= len(m.channels) {
		return nil
	}
	return &Pipe{id: id, out: m.inbox, quit: m.quit}
}

// Snapshot returns a consistent view of the latest completed round.
// Before the first round completes it returns the zero Snapshot.
func (m *Monitor) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.latest
}

// PersistErr returns the last persistence error, if any.
func (m *Monitor) PersistErr() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.persistErr
}

// Close stops the ingest loop and flushes state.
func (m *Monitor) Close() error {
	m.closeOnce.Do(func() {
		close(m.quit)
		m.wg.Wait()
		m.persist()
	})
	return m.PersistErr()
}

func (m *Monitor) run() {
	defer m.wg.Done()
	for {
		select {
		case s := <-m.inbox:
			m.enqueue(s)
			m.tryRounds()
		case u := <-m.configCh:
			m.applyConfig(u)
		case <-m.quit:
			return
		}
	}
}

func (m *Monitor) enqueue(s Sample) {
	if s.Channel < 0 || int(s.Channel) >= len(m.channels) {
		return
	}
	c := m.channels[s.Channel]
	c.pending = append(c.pending, s)
	if len(c.pending) > maxPending {
		c.pending = c.pending[len(c.pending)-maxPending:]
	}
}

// tryRounds completes as many rounds as the queued samples allow. Samples
// are aligned strictly by sequence number; stale samples that can no
// longer join any round are dropped.
func (m *Monitor) tryRounds() {
	for {
		for _, c := range m.channels {
			if len(c.pending) == 0 {
				return
			}
		}
		minHead := m.channels[0].pending[0].Seq
		for _, c := range m.channels[1:] {
			if seq := c.pending[0].Seq; seq < minHead {
				minHead = seq
			}
		}
		if minHead > m.nextSeq {
			m.nextSeq = minHead
		}
		for _, c := range m.channels {
			for len(c.pending) > 0 && c.pending[0].Seq < m.nextSeq {
				c.pending = c.pending[1:]
			}
		}
		aligned := true
		for _, c := range m.channels {
			if len(c.pending) == 0 || c.pending[0].Seq != m.nextSeq {
				aligned = false
				break
			}
		}
		if !aligned {
			// Round nextSeq can never complete: some channel already
			// moved past it (e.g. its queue evicted older samples).
			// Drop the blocking samples and skip the uncompletable
			// round instead of spinning on it.
			for _, c := range m.channels {
				if len(c.pending) > 0 && c.pending[0].Seq == m.nextSeq {
					c.pending = c.pending[1:]
				}
			}
			m.nextSeq++
			continue
		}
		samples := make([]Sample, len(m.channels))
		for i, c := range m.channels {
			samples[i] = c.pending[0]
			c.pending = c.pending[1:]
		}
		m.processRound(samples)
	}
}

// processRound evaluates one aligned round: vote over the participating
// channels, fold the verdict into every channel's health state, publish a
// consistent snapshot and persist. On return nextSeq has advanced past
// the processed round, so persisted state always points at the first
// unprocessed round and a restart never re-evaluates one.
func (m *Monitor) processRound(samples []Sample) {
	var participants []Sample
	for i, c := range m.channels {
		if c.participates() {
			participants = append(participants, samples[i])
		}
	}
	v := vote(participants, m.cfg.Threshold, m.cfg.Redundancy, m.cfg.MinVoters)

	// Health is evaluated against the current vote when it is valid, and
	// otherwise against the last valid output. The fallback lets the
	// system keep isolating a channel that disagrees with the established
	// output (e.g. a jump in a dual configuration) instead of stalling
	// with an unexplainable invalid output forever.
	reference, hasReference := v.Value, v.Valid
	if !hasReference && m.hasLastValid {
		reference, hasReference = m.lastValid, true
	}
	if hasReference {
		for i, c := range m.channels {
			d := samples[i].Value - reference
			if d < 0 {
				d = -d
			}
			c.record(d <= m.cfg.Threshold, m.cfg)
		}
	}
	if v.Valid {
		m.lastValid, m.hasLastValid = v.Value, true
	}
	for i, c := range m.channels {
		c.pushHistory(samples[i], m.cfg.HistorySize)
	}

	snap := Snapshot{Seq: m.nextSeq, Vote: v, Channels: make([]ChannelStatus, len(m.channels))}
	for i, c := range m.channels {
		snap.Channels[i] = c.status()
	}
	m.mu.Lock()
	m.latest = snap
	m.mu.Unlock()
	if m.onRound != nil {
		m.onRound(snap)
	}
	m.nextSeq++
	m.persist()
}

func (m *Monitor) applyConfig(u ConfigUpdate) {
	if u.Threshold != nil && *u.Threshold > 0 {
		m.cfg.Threshold = *u.Threshold
	}
	if u.RecoverWindow != nil && *u.RecoverWindow >= 1 {
		m.cfg.RecoverWindow = *u.RecoverWindow
	}
}

func (m *Monitor) persistedState() PersistedState {
	st := PersistedState{
		NextSeq:      m.nextSeq,
		LastValid:    m.lastValid,
		HasLastValid: m.hasLastValid,
		Channels:     make([]PersistedChannel, len(m.channels)),
	}
	for i, c := range m.channels {
		history := make([]Sample, len(c.history))
		copy(history, c.history)
		st.Channels[i] = PersistedChannel{
			ID:          c.id,
			Health:      c.health,
			MismatchRun: c.mismatchRun,
			MatchRun:    c.matchRun,
			History:     history,
		}
	}
	return st
}

func (m *Monitor) persist() {
	if m.store == nil {
		return
	}
	if err := m.store.Save(m.persistedState()); err != nil {
		m.mu.Lock()
		m.persistErr = err
		m.mu.Unlock()
	}
}

func (m *Monitor) restore(st PersistedState) {
	m.nextSeq = st.NextSeq
	m.lastValid = st.LastValid
	m.hasLastValid = st.HasLastValid
	for _, pc := range st.Channels {
		if pc.ID < 0 || int(pc.ID) >= len(m.channels) {
			continue
		}
		c := m.channels[pc.ID]
		c.health = pc.Health
		c.mismatchRun = pc.MismatchRun
		c.matchRun = pc.MatchRun
		c.history = append(c.history, pc.History...)
		if m.cfg.HistorySize >= 0 && len(c.history) > m.cfg.HistorySize {
			c.history = c.history[len(c.history)-m.cfg.HistorySize:]
		}
	}
}
