package testutil

import (
	"context"
	"fmt"
	"net/http/httptest"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

type MockS3 struct {
	Server *httptest.Server
	Client *s3.Client
	Bucket string
}

func StartMockS3(ctx context.Context, bucket string) (*MockS3, error) {
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}

	backend := s3mem.New()
	faker := gofakes3.New(backend)
	server := httptest.NewServer(faker.Server())

	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		server.Close()
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(server.URL)
	})

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		server.Close()
		return nil, fmt.Errorf("create bucket %q: %w", bucket, err)
	}

	return &MockS3{
		Server: server,
		Client: client,
		Bucket: bucket,
	}, nil
}

func (m *MockS3) Close() {
	if m == nil || m.Server == nil {
		return
	}
	m.Server.Close()
}
