package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type Persistent struct {
	CurrentTerm int32      `json:"current_term"`
	VotedFor    int32      `json:"voted_for"`
	Log         []LogEntry `json:"log"`
}

type LogEntry struct {
	Term  int32  `json:"term"`
	Index int32  `json:"index"`
	Cmd   string `json:"cmd"`
}

type Store struct {
	dir string
}

func New(dir string) *Store {
	os.MkdirAll(dir, 0o755)
	return &Store{dir: dir}
}

func (s *Store) path() string {
	return filepath.Join(s.dir, "state.json")
}

func (s *Store) Save(p *Persistent) error {
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path())
}

func (s *Store) Load() (*Persistent, error) {
	data, err := os.ReadFile(s.path())
	if os.IsNotExist(err) {
		return &Persistent{VotedFor: -1}, nil
	}
	if err != nil {
		return nil, err
	}
	var p Persistent
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &p, nil
}
