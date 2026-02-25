package kb

import (
	"context"
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoManifestStore(t *testing.T) {
	runManifestStoreTests(t, func(t *testing.T) ManifestStore {
		return newTestMongoManifestStore(t)
	})
}

func newTestMongoManifestStore(t *testing.T) *MongoManifestStore {
	t.Helper()

	uri := os.Getenv("KBCORE_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("KBCORE_TEST_MONGO_URI not set; skipping Mongo integration test")
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}

	ctx := context.Background()
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("mongo ping: %v", err)
	}

	dbName := "kbcore_test"
	collName := "manifests_" + t.Name()
	coll := client.Database(dbName).Collection(collName)

	// Clean up collection before and after test.
	_ = coll.Drop(ctx)
	t.Cleanup(func() {
		_ = coll.Drop(ctx)
		_ = client.Disconnect(ctx)
	})

	return NewMongoManifestStore(coll)
}
