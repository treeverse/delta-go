package action

// DomainMetadata stores domain-specific configuration in the Delta log.
// For row tracking, the domain is "delta.rowTracking" and the configuration
// is a JSON-encoded string containing the high watermark:
//
//	{"highWaterMark": <int64>}
type DomainMetadata struct {
	Domain        string `json:"domain"`
	Configuration string `json:"configuration"`
	Removed       bool   `json:"removed"`
}

func (d *DomainMetadata) Wrap() *SingleAction {
	return &SingleAction{DomainMetadata: d}
}

func (d *DomainMetadata) Json() (string, error) {
	return jsonString(d)
}
