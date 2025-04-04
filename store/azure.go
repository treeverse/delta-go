package store

import (
	"context"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util/path"
	"github.com/csimplestring/delta-go/iter"

	goblob "gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
)

func NewAzureBlobLogStore(logDir string, m *goblob.URLMux) (*AzureBlobLogStore, error) {
	// logDir is like: azblob:///a/b/c/_delta_log/, must end with /
	blobURL, err := path.ConvertToBlobURL(logDir)
	if err != nil {
		return nil, err
	}

	var bucket *goblob.Bucket
	if m == nil {
		bucket, err = goblob.OpenBucket(context.Background(), blobURL)
	} else {
		bucket, err = m.OpenBucket(context.Background(), blobURL)
	}
	if err != nil {
		return nil, err
	}

	logDir = strings.TrimPrefix(logDir, "azblob://")
	s := &baseStore{
		logDir: logDir,
		bucket: bucket,
		beforeWriteFn: func(asFunc func(interface{}) bool) error {
			var opt *azblob.UploadStreamOptions
			if asFunc(&opt) {
				opt.AccessConditions = &azblob.AccessConditions{
					ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)},
				}
			}
			return nil
		},
		writeErrorFn: func(err error, path string) error {
			var azError *azcore.ResponseError
			if bucket.ErrorAs(err, &azError) && bloberror.HasCode(azError, bloberror.BlobAlreadyExists) {
				return errno.FileAlreadyExists(path)
			}
			return err
		},
	}

	return &AzureBlobLogStore{
		logDir: logDir,
		s:      s,
	}, nil
}

type AzureBlobLogStore struct {
	logDir string
	s      *baseStore
}

func (a *AzureBlobLogStore) Root() string {
	return ""
}

// Read the given file and return an `Iterator` of lines, with line breaks removed from
// each line. Callers of this function are responsible to close the iterator if they are
// done with it.
func (a *AzureBlobLogStore) Read(path string) (iter.Iter[string], error) {
	path, err := a.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return nil, err
	}

	return a.s.Read(path)
}

// List the paths in the same directory that are lexicographically greater or equal to (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
func (a *AzureBlobLogStore) ListFrom(path string) (iter.Iter[*FileMeta], error) {
	path, err := a.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return nil, err
	}

	return a.s.ListFrom(path)
}

// Write the given `actions` to the given `path` with or without overwrite as indicated.
// Implementation must throw FileAlreadyExistsException exception if the file already
// exists and overwrite = false. Furthermore, if isPartialWriteVisible returns false,
// implementation must ensure that the entire file is made visible atomically, that is,
// it should not generate partial files.
func (a *AzureBlobLogStore) Write(path string, actions iter.Iter[string], overwrite bool) error {

	path, err := a.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return err
	}

	return a.s.Write(path, actions, overwrite)
}

// Resolve the fully qualified path for the given `path`.
func (a *AzureBlobLogStore) ResolvePathOnPhysicalStore(path string) (string, error) {
	return relativePath("azblob", a.logDir, path)
}

// Whether a partial write is visible for the underlying file system of `path`.
func (a *AzureBlobLogStore) IsPartialWriteVisible(path string) bool {
	return false
}

func (a *AzureBlobLogStore) Exists(path string) (bool, error) {
	return a.s.Exists(path)
}

func (a *AzureBlobLogStore) Create(path string) error {
	return a.s.Create(path)
}
