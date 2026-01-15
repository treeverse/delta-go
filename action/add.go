package action

import (
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

	// Protocol 3/7 fields for row tracking (passthrough only)
	BaseRowId               *int64 `json:"baseRowId,omitempty"`
	DefaultRowCommitVersion *int64 `json:"defaultRowCommitVersion,omitempty"`
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
