package action

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func int64ptr(v int64) *int64 { return &v }

func TestAddFile_RowTrackingFieldsRoundTrip(t *testing.T) {
	add := &AddFile{
		Path:                    "part-0001.parquet",
		DataChange:              true,
		PartitionValues:         map[string]string{},
		Size:                    1024,
		ModificationTime:        1700000000000,
		BaseRowId:               int64ptr(42),
		DefaultRowCommitVersion: int64ptr(7),
	}

	b, err := json.Marshal(add.Wrap())
	require.NoError(t, err)

	var sa SingleAction
	require.NoError(t, json.Unmarshal(b, &sa))
	require.NotNil(t, sa.Add)
	assert.Equal(t, int64ptr(42), sa.Add.BaseRowId)
	assert.Equal(t, int64ptr(7), sa.Add.DefaultRowCommitVersion)
}

func TestAddFile_RowTrackingFieldsAbsentWhenNil(t *testing.T) {
	add := &AddFile{
		Path:            "part-0001.parquet",
		DataChange:      true,
		PartitionValues: map[string]string{},
		Size:            1024,
		ModificationTime: 1700000000000,
	}

	b, err := json.Marshal(add.Wrap())
	require.NoError(t, err)

	// fields must be absent (omitempty) when nil
	assert.NotContains(t, string(b), "baseRowId")
	assert.NotContains(t, string(b), "defaultRowCommitVersion")
}

func TestRemoveFile_RowTrackingFieldsRoundTrip(t *testing.T) {
	ts := int64(1700000000000)
	remove := &RemoveFile{
		Path:                    "part-0001.parquet",
		DataChange:              true,
		DeletionTimestamp:       &ts,
		BaseRowId:               int64ptr(42),
		DefaultRowCommitVersion: int64ptr(7),
	}

	b, err := json.Marshal(remove.Wrap())
	require.NoError(t, err)

	var sa SingleAction
	require.NoError(t, json.Unmarshal(b, &sa))
	require.NotNil(t, sa.Remove)
	assert.Equal(t, int64ptr(42), sa.Remove.BaseRowId)
	assert.Equal(t, int64ptr(7), sa.Remove.DefaultRowCommitVersion)
}

func TestDomainMetadata_RoundTrip(t *testing.T) {
	dm := &DomainMetadata{
		Domain:        "delta.rowTracking",
		Configuration: `{"highWaterMark":100}`,
		Removed:       false,
	}

	b, err := json.Marshal(dm.Wrap())
	require.NoError(t, err)

	var sa SingleAction
	require.NoError(t, json.Unmarshal(b, &sa))
	require.NotNil(t, sa.DomainMetadata)
	assert.Equal(t, "delta.rowTracking", sa.DomainMetadata.Domain)
	assert.Equal(t, `{"highWaterMark":100}`, sa.DomainMetadata.Configuration)
	assert.False(t, sa.DomainMetadata.Removed)
}

func TestDomainMetadata_FromJson(t *testing.T) {
	raw := `{"domainMetadata":{"domain":"delta.rowTracking","configuration":"{\"highWaterMark\":200}","removed":false}}`

	a, err := FromJson(raw)
	require.NoError(t, err)
	dm, ok := a.(*DomainMetadata)
	require.True(t, ok, "expected *DomainMetadata, got %T", a)
	assert.Equal(t, "delta.rowTracking", dm.Domain)
	assert.Equal(t, `{"highWaterMark":200}`, dm.Configuration)
}
