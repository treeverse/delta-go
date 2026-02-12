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
