module github.com/csimplestring/delta-go

go 1.19

require (
	cloud.google.com/go/storage v1.35.1
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.9.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.2.0
	github.com/ahmetb/go-linq/v3 v3.2.0
	github.com/barweiss/go-tuple v1.1.1
	github.com/deckarep/golang-set/v2 v2.3.1
	github.com/fraugster/parquet-go v0.12.0
	github.com/google/uuid v1.4.0
	github.com/mitchellh/hashstructure/v2 v2.0.2
	github.com/repeale/fp-go v0.11.1
	github.com/rotisserie/eris v0.5.4
	github.com/samber/mo v1.10.0
	github.com/shopspring/decimal v1.3.1
	github.com/stretchr/testify v1.8.4
	github.com/ulule/deepcopier v0.0.0-20200430083143-45decc6639b6
	github.com/xhit/go-str2duration/v2 v2.1.0
	gocloud.dev v0.34.1-0.20231122211418-53ccd8db26a1
)

require (
	cloud.google.com/go v0.110.10 // indirect
	cloud.google.com/go/compute v1.23.3 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.1.5 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.4.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.5.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.2.0 // indirect
	github.com/apache/thrift v0.16.0 // indirect
	github.com/aws/aws-sdk-go v1.48.9 // indirect
	github.com/aws/aws-sdk-go-v2 v1.23.4 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.5.3 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.25.5 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.16.8 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.14.8 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.14.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.2.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.5.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.7.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.2.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.10.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.2.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.10.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.16.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.47.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.18.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.21.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.26.1 // indirect
	github.com/aws/smithy-go v1.18.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang-jwt/jwt/v5 v5.1.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/google/wire v0.5.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/pkg/browser v0.0.0-20210911075715-681adbf594b8 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	golang.org/x/crypto v0.15.0 // indirect
	golang.org/x/exp v0.0.0-20230321023759-10a507213a29 // indirect
	golang.org/x/net v0.18.0 // indirect
	golang.org/x/oauth2 v0.14.0 // indirect
	golang.org/x/sync v0.5.0 // indirect
	golang.org/x/sys v0.14.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/time v0.4.0 // indirect
	golang.org/x/xerrors v0.0.0-20231012003039-104605ab7028 // indirect
	google.golang.org/api v0.151.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto v0.0.0-20231120223509-83a465c0220f // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20231120223509-83a465c0220f // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231120223509-83a465c0220f // indirect
	google.golang.org/grpc v1.59.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/fraugster/parquet-go v0.12.0 => github.com/csimplestring/parquet-go v0.0.0-20230120063840-2107929f9cbe

replace gocloud.dev => github.com/google/go-cloud v0.34.1-0.20231122211418-53ccd8db26a1
