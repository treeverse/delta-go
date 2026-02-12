package action

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProtocol_JsonDeserialize_WriterFeaturesOnly(t *testing.T) {
	input := `{
		"minReaderVersion": 1,
		"minWriterVersion": 7,
		"writerFeatures": ["appendOnly", "changeDataFeed", "checkConstraints", "generatedColumns"]
	}`

	var p Protocol
	err := json.Unmarshal([]byte(input), &p)
	assert.NoError(t, err)
	assert.Equal(t, int32(1), p.MinReaderVersion)
	assert.Equal(t, int32(7), p.MinWriterVersion)
	assert.Nil(t, p.ReaderFeatures)
	assert.Equal(t, []string{"appendOnly", "changeDataFeed", "checkConstraints", "generatedColumns"}, p.WriterFeatures)
}

func TestProtocol_JsonDeserialize_BothFeatures(t *testing.T) {
	input := `{
		"minReaderVersion": 3,
		"minWriterVersion": 7,
		"readerFeatures": ["columnMapping"],
		"writerFeatures": ["columnMapping", "identityColumns"]
	}`

	var p Protocol
	err := json.Unmarshal([]byte(input), &p)
	assert.NoError(t, err)
	assert.Equal(t, int32(3), p.MinReaderVersion)
	assert.Equal(t, int32(7), p.MinWriterVersion)
	assert.Equal(t, []string{"columnMapping"}, p.ReaderFeatures)
	assert.Equal(t, []string{"columnMapping", "identityColumns"}, p.WriterFeatures)
}

func TestProtocol_JsonDeserialize_NoFeatures(t *testing.T) {
	input := `{"minReaderVersion": 1, "minWriterVersion": 2}`

	var p Protocol
	err := json.Unmarshal([]byte(input), &p)
	assert.NoError(t, err)
	assert.Equal(t, int32(1), p.MinReaderVersion)
	assert.Equal(t, int32(2), p.MinWriterVersion)
	assert.Nil(t, p.ReaderFeatures)
	assert.Nil(t, p.WriterFeatures)
}

func TestProtocol_JsonSerialize_WriterFeatures(t *testing.T) {
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
	assert.Equal(t, p.MinReaderVersion, roundtrip.MinReaderVersion)
	assert.Equal(t, p.MinWriterVersion, roundtrip.MinWriterVersion)
	assert.Nil(t, roundtrip.ReaderFeatures)
	assert.Equal(t, p.WriterFeatures, roundtrip.WriterFeatures)
}

func TestProtocol_JsonDeserialize_WrappedInSingleAction(t *testing.T) {
	input := `{"protocol":{"minReaderVersion":1,"minWriterVersion":7,"writerFeatures":["appendOnly","changeDataFeed"]}}`

	a, err := FromJson(input)
	assert.NoError(t, err)

	p, ok := a.(*Protocol)
	assert.True(t, ok)
	assert.Equal(t, int32(1), p.MinReaderVersion)
	assert.Equal(t, int32(7), p.MinWriterVersion)
	assert.Equal(t, []string{"appendOnly", "changeDataFeed"}, p.WriterFeatures)
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
