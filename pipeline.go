package redundancy

// Pipe is the write end of an in-memory channel feeding the Monitor.
// Multiple pipes may be written concurrently.
type Pipe struct {
	id   ChannelID
	out  chan<- Sample
	quit <-chan struct{}
}

// Write enqueues one sample. The Seq must be set by the caller; the
// Channel field is overridden with the pipe's channel id.
// It blocks while the monitor's intake buffer is full and returns
// immediately once the monitor is closed.
func (p *Pipe) Write(s Sample) {
	s.Channel = p.id
	select {
	case p.out <- s:
	case <-p.quit:
	}
}
