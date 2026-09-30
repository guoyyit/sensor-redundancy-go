package redundancy

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrNoState is returned by Store.Load when no state was persisted yet.
var ErrNoState = errors.New("redundancy: no persisted state")

// PersistedChannel is the durable state of one channel.
type PersistedChannel struct {
	ID          ChannelID
	Health      HealthState
	MismatchRun int
	MatchRun    int
	History     []Sample
}

// PersistedState is the durable state of the whole Monitor.
type PersistedState struct {
	NextSeq      uint64
	LastValid    float64
	HasLastValid bool
	Channels     []PersistedChannel
}

// Store persists monitor state for crash recovery.
type Store interface {
	Load() (PersistedState, error)
	Save(PersistedState) error
}

// FileStore is a Store backed by a single JSON file, written atomically.
type FileStore struct {
	path string
}

// NewFileStore creates a Store persisting to path.
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

func (s *FileStore) Load() (PersistedState, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return PersistedState{}, ErrNoState
	}
	if err != nil {
		return PersistedState{}, err
	}
	var st PersistedState
	if err := json.Unmarshal(data, &st); err != nil {
		return PersistedState{}, fmt.Errorf("redundancy: corrupt state file: %w", err)
	}
	return st, nil
}

// Save writes the state atomically: a temporary file in the same
// directory is fully written and then renamed over the target, so a crash
// can never leave a half-written state file behind.
func (s *FileStore) Save(st PersistedState) error {
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".redundancy-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}
