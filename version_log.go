package deltago

import (
	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/iter"
	"github.com/csimplestring/delta-go/store"
)

type VersionLog interface {
	Version() int64
	Actions() ([]action.Action, error)
	ActionIter() (iter.Iter[action.Action], error)
}

var _ VersionLog = &InMemVersionLog{}
var _ VersionLog = &MemOptimizedVersionLog{}

type InMemVersionLog struct {
	version int64
	actions []action.Action
}

func (v *InMemVersionLog) Version() int64 {
	return v.version
}

func (v *InMemVersionLog) Actions() ([]action.Action, error) {
	return v.actions, nil
}

func (v *InMemVersionLog) ActionIter() (iter.Iter[action.Action], error) {
	return iter.FromSlice(v.actions), nil
}

type MemOptimizedVersionLog struct {
	version int64
	path    string
	store   store.Store
}

func (m *MemOptimizedVersionLog) Version() int64 {
	return m.version
}

func (m *MemOptimizedVersionLog) Actions() ([]action.Action, error) {
	i, err := m.store.Read(m.path)
	if err != nil {
		return nil, err
	}
	defer i.Close()

	return iter.Map(i, func(t string) (action.Action, error) {
		return action.FromJson(t)
	})
}

func (m *MemOptimizedVersionLog) ActionIter() (iter.Iter[action.Action], error) {
	i, err := m.store.Read(m.path)
	if err != nil {
		return nil, err
	}
	defer i.Close()

	mapIter := &iter.MapIter[string, action.Action]{
		It: i,
		Mapper: func(s string) (action.Action, error) {
			return action.FromJson(s)
		},
	}
	return mapIter, nil
}

type MemOptimizedCheckpoint struct {
	version int64
	path    string
	store   store.Store
	cr      *checkpointReader
}

func (m *MemOptimizedCheckpoint) Version() int64 {
	return m.version * -1
}

func (m *MemOptimizedCheckpoint) Actions() ([]action.Action, error) {
	cr := *(m.cr)
	i, err := cr.Read(m.path)
	if err != nil {
		return nil, err
	}
	defer i.Close()

	return iter.Map(i, func(a action.Action) (action.Action, error) {
		if a.Wrap().MetaData != nil {
			md := a.Wrap().MetaData
			if md.Configuration == nil {
				md.Configuration = map[string]string{}
			}
			if md.PartitionColumns == nil {
				md.PartitionColumns = []string{}
			}
			if md.Format.Options == nil {
				md.Format.Options = map[string]string{}
			}
		}
		return a, nil
	})
}

func (m *MemOptimizedCheckpoint) ActionIter() (iter.Iter[action.Action], error) {
	cr := *(m.cr)
	return cr.Read(m.path)
}
