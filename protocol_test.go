package deltago

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLog_ReaderVersion3_Success(t *testing.T) {
	// This test verifies that reading a table with minReaderVersion=3 and minWriterVersion=7 works
	// after bumping the supported protocol versions.

	// We use the existing newTestLogCases helper which scans tests/golden
	for _, tt := range newTestLogCases("file") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			defer tt.clean()

			log, err := tt.getLog("deltalog-reader-version-3")
			assert.NoError(t, err, "Should be able to read table with minReaderVersion=3 and minWriterVersion=7")

			// Verify we can get the snapshot and protocol
			snapshot, err := log.Snapshot()
			assert.NoError(t, err)

			protocol, err := snapshot.Protocol()
			assert.NoError(t, err)
			assert.Equal(t, int32(3), protocol.MinReaderVersion)
			assert.Equal(t, int32(7), protocol.MinWriterVersion)
		})
	}
}
