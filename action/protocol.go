package action

import "slices"

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
		slices.Equal(p.ReaderFeatures, other.ReaderFeatures) &&
		slices.Equal(p.WriterFeatures, other.WriterFeatures)
}

func DefaultProtocol() *Protocol {
	return &Protocol{
		MinReaderVersion: 1,
		MinWriterVersion: 2,
	}
}
