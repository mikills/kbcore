package main

import (
	"context"
	"fmt"
	"log"

	"github.com/mikills/kbcore/kb"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// 1. First load downloads from S3 (slower)
// 2. Subsequent loads use local cache (fast)
// 3. Uploads use optimistic concurrency with ETags
// 4. Cache eviction removes local files but keeps S3 version
// 5. Multiple instances can share the same S3 bucket
func main() {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	s3Client := s3.NewFromConfig(cfg)

	blobStore := kb.NewS3BlobStore(s3Client, "my-knowledge-bases", "prod/")

	knowledgeBase := kb.NewKB(blobStore, "/tmp/kb-cache")

	// Use KB normally - it will download from S3 on first access
	// and cache locally for subsequent accesses
	db, err := knowledgeBase.Load(ctx, "my-kb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("Successfully loaded KB from S3")
}
