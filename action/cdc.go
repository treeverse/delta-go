package action

import (
	"net/url"
)

type AddCDCFile struct {
	Path            string            `json:"path"`
	DataChange      bool              `json:"dataChange"`
	PartitionValues map[string]string `json:"partitionValues"`
	Size            int64             `json:"size"`
	Tags            map[string]string `json:"tags"`
}

func (a *AddCDCFile) IsDataChanged() bool {
	return a.DataChange
}

func (a *AddCDCFile) PathAsUri() (*url.URL, error) {
	return url.Parse(a.Path)
}

func (a *AddCDCFile) Wrap() *SingleAction {
	return &SingleAction{Cdc: a}
}

func (a *AddCDCFile) Json() (string, error) {
	return jsonString(a)
}
