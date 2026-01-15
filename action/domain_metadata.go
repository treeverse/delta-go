package action

// DomainMetadata represents domain-specific metadata in protocol 3/7
// This is used for features like liquid clustering and other advanced table features
type DomainMetadata struct {
	Domain         string         `json:"domain"`
	Configuration  map[string]any `json:"configuration"`
	Removed        bool           `json:"removed"`
}

func (d *DomainMetadata) Wrap() *SingleAction {
	return &SingleAction{DomainMetadata: d}
}

func (d *DomainMetadata) Json() (string, error) {
	return jsonString(d)
}
