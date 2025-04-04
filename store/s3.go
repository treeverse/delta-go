package store

import (
	"context"
	"net/url"
	"strings"
	"sync"

	"gocloud.dev/blob"

	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util/path"
	"github.com/csimplestring/delta-go/iter"
	"github.com/rotisserie/eris"
)

// Note: currently s3 log store only supports single driver.
// TODO: support multi-drivers writes
// see https://delta.io/blog/2022-05-18-multi-cluster-writes-to-delta-lake-storage-in-s3/
func NewS3LogStore(logDir string, m *blob.URLMux) (*S3SingleDriverLogStore, error) {
	// logDir is like: s3:///a/b/c/_delta_log/, must end with "/"
	blobURL, err := path.ConvertToBlobURL(logDir)
	if err != nil {
		return nil, err
	}

	var bucket *blob.Bucket
	if m == nil {
		bucket, err = blob.OpenBucket(context.Background(), blobURL)
	} else {
		bucket, err = m.OpenBucket(context.Background(), blobURL)
	}

	if err != nil {
		return nil, err
	}

	logDir = strings.TrimPrefix(logDir, "s3://")
	s := &baseStore{
		logDir: logDir,
		bucket: bucket,
		beforeWriteFn: func(asFunc func(interface{}) bool) error {
			return nil
		},
		writeErrorFn: func(err error, path string) error {
			return err
		},
	}

	return &S3SingleDriverLogStore{
		logDir: logDir,
		s:      s,
	}, nil
}

func NewS3CompatLogStore(u *url.URL, m *blob.URLMux) (*S3SingleDriverLogStore, error) {
	var bucket *blob.Bucket
	var err error
	if m == nil {
		bucket, err = blob.OpenBucket(context.Background(), u.String())
	} else {
		bucket, err = m.OpenBucket(context.Background(), u.String())
	}
	if err != nil {
		return nil, err
	}
	logDir := handleLogDirPath(u.Path)
	bucket = blob.PrefixedBucket(bucket, logDir)
	s := &baseStore{
		logDir: logDir,
		bucket: bucket,
		beforeWriteFn: func(asFunc func(interface{}) bool) error {
			return nil
		},
		writeErrorFn: func(err error, path string) error {
			return err
		},
	}

	return &S3SingleDriverLogStore{
		logDir: logDir,
		s:      s,
	}, nil
}

func handleLogDirPath(path string) string {
	path = strings.TrimPrefix(path, "/")
	if strings.HasSuffix(path, "_delta_log/") {
		return path
	} else if strings.HasSuffix(path, "_delta_log") {
		return path + "/"
	} else if strings.HasSuffix(path, "/") {
		return path + "_delta_log/"
	} else {
		return path + "/_delta_log/"
	}
}

type S3SingleDriverLogStore struct {
	logDir string
	s      *baseStore
	mu     sync.Mutex
}

func (a *S3SingleDriverLogStore) Root() string {
	return ""
}

// Read the given file and return an `Iterator` of lines, with line breaks removed from
// each line. Callers of this function are responsible to close the iterator if they are
// done with it.
func (a *S3SingleDriverLogStore) Read(path string) (iter.Iter[string], error) {
	path, err := a.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return nil, err
	}

	return a.s.Read(path)
}

// List the paths in the same directory that are lexicographically greater or equal to (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
func (a *S3SingleDriverLogStore) ListFrom(path string) (iter.Iter[*FileMeta], error) {
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
func (a *S3SingleDriverLogStore) Write(path string, actions iter.Iter[string], overwrite bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	path, err := a.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return err
	}

	if !overwrite {
		ok, err := a.s.Exists(path)
		if err != nil {
			return eris.Wrap(err, "s3 failed to check existing file "+path)
		}
		if ok {
			return errno.FileAlreadyExists(path)
		}
	}

	return a.s.Write(path, actions, overwrite)
}

// Resolve the fully qualified path for the given `path`.
func (a *S3SingleDriverLogStore) ResolvePathOnPhysicalStore(path string) (string, error) {
	return relativePath("s3", a.logDir, path)
}

// Whether a partial write is visible for the underlying file system of `path`.
func (a *S3SingleDriverLogStore) IsPartialWriteVisible(path string) bool {
	return false
}

func (a *S3SingleDriverLogStore) Exists(path string) (bool, error) {
	return a.s.Exists(path)
}

func (a *S3SingleDriverLogStore) Create(path string) error {
	return a.s.Create(path)
}
