package kb

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mongoManifestDoc is the BSON document schema for manifest storage.
type mongoManifestDoc struct {
	ID        string                `json:"_id" bson:"_id"`
	Version   string                `json:"version" bson:"version"`
	Manifest  SnapshotShardManifest `json:"manifest" bson:"manifest"`
	UpdatedAt time.Time             `json:"updated_at" bson:"updated_at"`
}

// MongoManifestStore implements ManifestStore backed by a MongoDB collection.
// The caller owns the mongo.Client lifecycle.
type MongoManifestStore struct {
	Collection *mongo.Collection
}

// NewMongoManifestStore creates a MongoManifestStore from a *mongo.Collection.
func NewMongoManifestStore(collection *mongo.Collection) *MongoManifestStore {
	return &MongoManifestStore{Collection: collection}
}

func (s *MongoManifestStore) Get(ctx context.Context, kbID string) (*ManifestDocument, error) {
	var doc mongoManifestDoc
	err := s.Collection.FindOne(ctx, bson.M{"_id": kbID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrManifestNotFound
		}
		return nil, err
	}
	return &ManifestDocument{
		Manifest: doc.Manifest,
		Version:  doc.Version,
	}, nil
}

func (s *MongoManifestStore) HeadVersion(ctx context.Context, kbID string) (string, error) {
	var doc struct {
		Version string `bson:"version" json:"version"`
	}
	err := s.Collection.FindOne(ctx, bson.M{"_id": kbID},
		options.FindOne().SetProjection(bson.M{"version": 1}),
	).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", nil
		}
		return "", err
	}
	return doc.Version, nil
}

func (s *MongoManifestStore) UpsertIfMatch(ctx context.Context, kbID string, manifest SnapshotShardManifest, expectedVersion string) (string, error) {
	newVersion := uuid.New().String()
	doc := mongoManifestDoc{
		ID:        kbID,
		Version:   newVersion,
		Manifest:  manifest,
		UpdatedAt: time.Now().UTC(),
	}

	if expectedVersion == "" {
		// Insert-only: reject if a document already exists for this kbID.
		_, err := s.Collection.InsertOne(ctx, doc)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return "", ErrBlobVersionMismatch
			}
			return "", err
		}
		return newVersion, nil
	}

	// CAS: only replace if current version matches.
	res, err := s.Collection.ReplaceOne(ctx,
		bson.M{"_id": kbID, "version": expectedVersion},
		doc,
	)
	if err != nil {
		return "", err
	}
	if res.MatchedCount == 0 {
		return "", ErrBlobVersionMismatch
	}
	return newVersion, nil
}

func (s *MongoManifestStore) Delete(ctx context.Context, kbID string) error {
	_, err := s.Collection.DeleteOne(ctx, bson.M{"_id": kbID})
	return err
}
