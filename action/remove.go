package action

import (
	"encoding/json"
	"net/url"

	"github.com/ulule/deepcopier"
)

type RemoveFile struct {
	Path                 string            `json:"path"`
	DataChange           bool              `json:"dataChange"`
	DeletionTimestamp    *int64            `json:"deletionTimestamp,omitempty"`
	ExtendedFileMetadata bool              `json:"extendedFileMetadata,omitempty"`
	PartitionValues      map[string]string `json:"partitionValues,omitempty"`
	Size                 *int64            `json:"size,omitempty"`
	Tags                 map[string]string `json:"tags,omitempty"`

	// Preserve unknown/new fields written by newer Delta protocol versions.
	Extra map[string]json.RawMessage `json:"-"`
}

func (r *RemoveFile) IsDataChanged() bool {
	return r.DataChange
}

func (r *RemoveFile) PathAsUri() (*url.URL, error) {
	return url.Parse(r.Path)
}

func (r *RemoveFile) Wrap() *SingleAction {
	return &SingleAction{Remove: r}
}

func (r *RemoveFile) Json() (string, error) {
	return jsonString(r)
}

func (r *RemoveFile) UnmarshalJSON(b []byte) error {
	type Alias RemoveFile

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	var alias Alias
	if err := json.Unmarshal(b, &alias); err != nil {
		return err
	}
	*r = RemoveFile(alias)

	delete(raw, "path")
	delete(raw, "dataChange")
	delete(raw, "deletionTimestamp")
	delete(raw, "extendedFileMetadata")
	delete(raw, "partitionValues")
	delete(raw, "size")
	delete(raw, "tags")
	if len(raw) > 0 {
		r.Extra = raw
	}
	return nil
}

func (r *RemoveFile) MarshalJSON() ([]byte, error) {
	type Alias RemoveFile
	alias := Alias(*r)

	b, err := json.Marshal(alias)
	if err != nil {
		return nil, err
	}
	if len(r.Extra) == 0 {
		return b, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	for k, v := range r.Extra {
		if _, exists := m[k]; !exists {
			m[k] = v
		}
	}
	return json.Marshal(m)
}

func (r *RemoveFile) DelTimestamp() int64 {
	if r.DeletionTimestamp == nil {
		return 0
	}
	return *r.DeletionTimestamp
}

func (r *RemoveFile) Copy(dataChange bool, path string) *RemoveFile {
	dst := &RemoveFile{}
	deepcopier.Copy(r).To(dst)
	dst.Path = path
	dst.DataChange = dataChange
	return dst
}
