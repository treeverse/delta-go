package storage

import (
	"context"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

type AWSProperties struct {
	Region         string
	ForcePathStyle bool
	CredsProvider  aws.CredentialsProvider
	Endpoint       string
}

type S3CompatBucketURLOpener struct {
	cfg      aws.Config
	awsProps AWSProperties
}

func NewS3CompatBucketURLOpener(cfg aws.Config, awsProps AWSProperties) S3CompatBucketURLOpener {
	return S3CompatBucketURLOpener{cfg, awsProps}
}

func (cbuo S3CompatBucketURLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	clientV2 := NewS3ClientV2(cbuo.cfg, &cbuo.awsProps)
	return s3blob.OpenBucketV2(ctx, clientV2, u.Host, nil)
}

// RegisterS3CompatBucketURLOpener is used to associate a scheme with a BucketURLOpener.
// scheme is the URL scheme that will be used to identify the registered BucketURLOpener with a given URL.
// awsProps specify common S3-compatible storage configurations.
// Example:
//
//		awsProps := AWSProperties{
//			Region: "us-west-2",
//			ForcePathStyle: true,
//			Endpoint: "http://localhost:8090",
//			CredsProvider: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
//				return aws.Credentials{
//					AccessKeyID:     "EXAMPLEACCESSKEY",
//					SecretAccessKey: "EXAMPLESECRETACCESSKEY",
//				}, nil
//			}),
//		}
//		RegisterS3CompatBucketURLOpener("myscheme", &awsProps)
//
//	 // 'blob.OpenBucket' uses the registered BucketURLOpener to retrieve a bucket client:
//		b, err := blob.OpenBucket(context.Background(), "myscheme://my-bucket/some/path")
func RegisterS3CompatBucketURLOpener(scheme string, awsProps *AWSProperties, m *blob.URLMux) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
		}
	}()
	ctx := context.Background()
	cfg, _ := GenerateConfig(ctx, awsProps)
	cbuo := S3CompatBucketURLOpener{
		cfg:      cfg,
		awsProps: *awsProps,
	}
	if m == nil {
		blob.DefaultURLMux().RegisterBucket(scheme, &cbuo)
	} else {
		m.RegisterBucket(scheme, &cbuo)
	}
}

func GenerateConfig(ctx context.Context, awsProps *AWSProperties) (aws.Config, error) {
	conf, err := config.LoadDefaultConfig(ctx,
		config.WithDefaultRegion("us-east-1"),
		config.WithRegion(awsProps.Region),
		config.WithCredentialsProvider(awsProps.CredsProvider),
	)
	if err != nil {
		return aws.Config{}, err
	}
	return conf, nil
}

func NewS3ClientV2(cfg aws.Config, awsProps *AWSProperties) *s3.Client {
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(awsProps.Endpoint)
		o.UsePathStyle = awsProps.ForcePathStyle
	})
}
