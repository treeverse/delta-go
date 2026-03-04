package deltago

import (
	"testing"

	"github.com/csimplestring/delta-go/action"
	"github.com/stretchr/testify/assert"
)

func TestAssertProtocolRead_V1Reader_V7Writer_Passes(t *testing.T) {
	p := &action.Protocol{
		MinReaderVersion: 1,
		MinWriterVersion: 7,
		WriterFeatures:   []string{"appendOnly", "changeDataFeed", "checkConstraints", "generatedColumns"},
	}
	err := assertProtocolRead(p)
	assert.NoError(t, err)
}

func TestAssertProtocolRead_V1Reader_V2Writer_Passes(t *testing.T) {
	p := &action.Protocol{
		MinReaderVersion: 1,
		MinWriterVersion: 2,
	}
	err := assertProtocolRead(p)
	assert.NoError(t, err)
}

func TestAssertProtocolRead_V2Reader_Rejected(t *testing.T) {
	p := &action.Protocol{
		MinReaderVersion: 2,
		MinWriterVersion: 7,
		WriterFeatures:   []string{"columnMapping"},
	}
	err := assertProtocolRead(p)
	assert.Error(t, err)
}

func TestAssertProtocolRead_V3Reader_Rejected(t *testing.T) {
	p := &action.Protocol{
		MinReaderVersion: 3,
		MinWriterVersion: 7,
		ReaderFeatures:   []string{"columnMapping"},
		WriterFeatures:   []string{"columnMapping", "identityColumns"},
	}
	err := assertProtocolRead(p)
	assert.Error(t, err)
}

func TestAssertProtocolRead_NilProtocol_Passes(t *testing.T) {
	err := assertProtocolRead(nil)
	assert.NoError(t, err)
}

// Protocol 3/7 with empty readerFeatures — the exact state after
// DROP FEATURE deletionVectors when other writerFeatures remain.
// minReaderVersion stays 3 but there are no actual reader requirements.
func TestAssertProtocolRead_V3EmptyReaderFeatures_Passes(t *testing.T) {
	p := &action.Protocol{
		MinReaderVersion: 3,
		MinWriterVersion: 7,
		ReaderFeatures:   []string{},
		WriterFeatures:   []string{"changeDataFeed", "invariants"},
	}
	err := assertProtocolRead(p)
	assert.NoError(t, err)
}

// Protocol 3/7 with deletionVectors still in readerFeatures — must be blocked.
func TestAssertProtocolRead_V3DeletionVectors_Rejected(t *testing.T) {
	p := &action.Protocol{
		MinReaderVersion: 3,
		MinWriterVersion: 7,
		ReaderFeatures:   []string{"deletionVectors"},
		WriterFeatures:   []string{"deletionVectors", "changeDataFeed"},
	}
	err := assertProtocolRead(p)
	assert.Error(t, err)
}

// Protocol 3/7 with only supported reader features — passes.
func TestAssertProtocolRead_V3SupportedReaderFeatures_Passes(t *testing.T) {
	p := &action.Protocol{
		MinReaderVersion: 3,
		MinWriterVersion: 7,
		ReaderFeatures:   []string{"timestampNtz", "rowTracking"},
		WriterFeatures:   []string{"rowTracking"},
	}
	err := assertProtocolRead(p)
	assert.NoError(t, err)
}
