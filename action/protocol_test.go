package action

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProtocol_JsonDeserialize(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		readerVersion  int32
		writerVersion  int32
		readerFeatures []string
		writerFeatures []string
	}{
		{
			name:           "writer features only",
			input:          `{"minReaderVersion":1,"minWriterVersion":7,"writerFeatures":["appendOnly","changeDataFeed","checkConstraints","generatedColumns"]}`,
			readerVersion:  1,
			writerVersion:  7,
			readerFeatures: nil,
			writerFeatures: []string{"appendOnly", "changeDataFeed", "checkConstraints", "generatedColumns"},
		},
		{
			name:           "both features",
			input:          `{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["columnMapping"],"writerFeatures":["columnMapping","identityColumns"]}`,
			readerVersion:  3,
			writerVersion:  7,
			readerFeatures: []string{"columnMapping"},
			writerFeatures: []string{"columnMapping", "identityColumns"},
		},
		{
			name:           "no features",
			input:          `{"minReaderVersion":1,"minWriterVersion":2}`,
			readerVersion:  1,
			writerVersion:  2,
			readerFeatures: nil,
			writerFeatures: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p Protocol
			err := json.Unmarshal([]byte(tt.input), &p)
			assert.NoError(t, err)
			assert.Equal(t, tt.readerVersion, p.MinReaderVersion)
			assert.Equal(t, tt.writerVersion, p.MinWriterVersion)
			assert.Equal(t, tt.readerFeatures, p.ReaderFeatures)
			assert.Equal(t, tt.writerFeatures, p.WriterFeatures)
		})
	}
}

func TestProtocol_JsonRoundTrip(t *testing.T) {
	p := &Protocol{
		MinReaderVersion: 1,
		MinWriterVersion: 7,
		WriterFeatures:   []string{"appendOnly", "changeDataFeed"},
	}

	b, err := json.Marshal(p)
	assert.NoError(t, err)

	var roundtrip Protocol
	err = json.Unmarshal(b, &roundtrip)
	assert.NoError(t, err)
	assert.True(t, p.Equals(&roundtrip))
}

func TestProtocol_Equals(t *testing.T) {
	tests := []struct {
		name     string
		a        *Protocol
		b        *Protocol
		expected bool
	}{
		{
			name:     "both nil features",
			a:        &Protocol{MinReaderVersion: 1, MinWriterVersion: 2},
			b:        &Protocol{MinReaderVersion: 1, MinWriterVersion: 2},
			expected: true,
		},
		{
			name:     "same writer features",
			a:        &Protocol{MinReaderVersion: 1, MinWriterVersion: 7, WriterFeatures: []string{"appendOnly", "changeDataFeed"}},
			b:        &Protocol{MinReaderVersion: 1, MinWriterVersion: 7, WriterFeatures: []string{"appendOnly", "changeDataFeed"}},
			expected: true,
		},
		{
			name:     "different writer features",
			a:        &Protocol{MinReaderVersion: 1, MinWriterVersion: 7, WriterFeatures: []string{"appendOnly"}},
			b:        &Protocol{MinReaderVersion: 1, MinWriterVersion: 7, WriterFeatures: []string{"changeDataFeed"}},
			expected: false,
		},
		{
			name:     "one has writer features other does not",
			a:        &Protocol{MinReaderVersion: 1, MinWriterVersion: 7, WriterFeatures: []string{"appendOnly"}},
			b:        &Protocol{MinReaderVersion: 1, MinWriterVersion: 7},
			expected: false,
		},
		{
			name: "same reader and writer features",
			a: &Protocol{MinReaderVersion: 3, MinWriterVersion: 7,
				ReaderFeatures: []string{"columnMapping"}, WriterFeatures: []string{"columnMapping"}},
			b: &Protocol{MinReaderVersion: 3, MinWriterVersion: 7,
				ReaderFeatures: []string{"columnMapping"}, WriterFeatures: []string{"columnMapping"}},
			expected: true,
		},
		{
			name: "different reader features",
			a: &Protocol{MinReaderVersion: 3, MinWriterVersion: 7,
				ReaderFeatures: []string{"columnMapping"}, WriterFeatures: []string{"columnMapping"}},
			b: &Protocol{MinReaderVersion: 3, MinWriterVersion: 7,
				ReaderFeatures: []string{"deletionVectors"}, WriterFeatures: []string{"columnMapping"}},
			expected: false,
		},
		{
			name:     "nil vs other",
			a:        &Protocol{MinReaderVersion: 1, MinWriterVersion: 2},
			b:        nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.a.Equals(tt.b))
		})
	}
}
