package action

import (
	"encoding/json"
	"net/url"
	"time"

	"github.com/ulule/deepcopier"
)

type AddFile struct {
	Path             string            `json:"path"`
	DataChange       bool              `json:"dataChange"`
	PartitionValues  map[string]string `json:"partitionValues"`
	Size             int64             `json:"size"`
	ModificationTime int64             `json:"modificationTime"`
	Stats            string            `json:"stats,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`

	// Preserve unknown/new fields written by newer Delta protocol versions
	// (e.g. deletion vectors, row tracking fields, etc.) so we can round-trip logs.
	Extra map[string]json.RawMessage `json:"-"`
}

func (a *AddFile) IsDataChanged() bool {
	return a.DataChange
}

func (a *AddFile) PathAsUri() (*url.URL, error) {
	return url.Parse(a.Path)
}

func (a *AddFile) Wrap() *SingleAction {
	return &SingleAction{Add: a}
}

func (a *AddFile) Json() (string, error) {
	return jsonString(a)
}

func (a *AddFile) UnmarshalJSON(b []byte) error {
	type Alias AddFile

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	var alias Alias
	if err := json.Unmarshal(b, &alias); err != nil {
		return err
	}
	*a = AddFile(alias)

	// Strip known keys, keep the rest.
	delete(raw, "path")
	delete(raw, "dataChange")
	delete(raw, "partitionValues")
	delete(raw, "size")
	delete(raw, "modificationTime")
	delete(raw, "stats")
	delete(raw, "tags")
	if len(raw) > 0 {
		a.Extra = raw
	}
	return nil
}

func (a *AddFile) MarshalJSON() ([]byte, error) {
	type Alias AddFile
	alias := Alias(*a)

	// marshal known fields
	b, err := json.Marshal(alias)
	if err != nil {
		return nil, err
	}
	if len(a.Extra) == 0 {
		return b, nil
	}
	// merge extras
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	for k, v := range a.Extra {
		// do not overwrite known keys
		if _, exists := m[k]; !exists {
			m[k] = v
		}
	}
	return json.Marshal(m)
}

func (a *AddFile) Remove() *RemoveFile {
	return a.RemoveWithTimestamp(nil, nil)
}

func (a *AddFile) RemoveWithTimestamp(ts *int64, dataChange *bool) *RemoveFile {
	if ts == nil {
		now := time.Now().UnixMilli()
		ts = &now
	}
	if dataChange == nil {
		dc := true
		dataChange = &dc
	}

	return &RemoveFile{
		Path:              a.Path,
		DeletionTimestamp: ts,
		DataChange:        *dataChange,
	}
}

func (a *AddFile) Copy(dataChange bool, path string) *AddFile {
	dst := &AddFile{}
	deepcopier.Copy(a).To(dst)
	dst.Path = path
	dst.DataChange = dataChange
	return dst
}
