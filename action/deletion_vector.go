package action

// DeletionVector represents a deletion vector that marks specific rows as deleted
// within a Parquet data file without rewriting it. It is stored as part of an AddFile
// action in the Delta log.
//
// See: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors
type DeletionVector struct {
	// StorageType indicates where the deletion vector is stored:
	//   "u" = UUID-based file at <tableRoot>/<pathOrInlineDv>.bin
	//   "p" = absolute path given by pathOrInlineDv
	//   "i" = inline, pathOrInlineDv contains the base85-encoded RoaringBitmap
	StorageType string `json:"storageType"`

	// PathOrInlineDv is the base85-encoded UUID (for "u"), absolute path (for "p"),
	// or base85-encoded serialized RoaringBitmap (for "i").
	PathOrInlineDv string `json:"pathOrInlineDv"`

	// Offset is the optional byte offset into the file where the DV data begins.
	// Only used for "u" and "p" storage types.
	Offset *int32 `json:"offset,omitempty"`

	// SizeInBytes is the size of the serialized DV data in bytes.
	SizeInBytes int32 `json:"sizeInBytes"`

	// Cardinality is the number of rows marked as deleted.
	Cardinality int64 `json:"cardinality"`
}
