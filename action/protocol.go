package action

type Protocol struct {
	MinReaderVersion int32    `json:"minReaderVersion"`
	MinWriterVersion int32    `json:"minWriterVersion"`
	ReaderFeatures   []string `json:"readerFeatures,omitempty"`
	WriterFeatures   []string `json:"writerFeatures,omitempty"`
}

func (p *Protocol) Wrap() *SingleAction {
	return &SingleAction{Protocol: p}
}

func (p *Protocol) Json() (string, error) {
	return jsonString(p)
}

func (p *Protocol) Equals(other *Protocol) bool {
	if other == nil {
		return false
	}
	return p.MinReaderVersion == other.MinReaderVersion &&
		p.MinWriterVersion == other.MinWriterVersion &&
		stringSlicesEqual(p.ReaderFeatures, other.ReaderFeatures) &&
		stringSlicesEqual(p.WriterFeatures, other.WriterFeatures)
}

func DefaultProtocol() *Protocol {
	return &Protocol{
		MinReaderVersion: 1,
		MinWriterVersion: 2,
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
