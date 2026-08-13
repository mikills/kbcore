package eventing

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mongoEventDoc is the BSON shape for Event persistence.
type mongoEventDoc struct {
	ID             string    `bson:"_id"` // event_id
	KBID           string    `bson:"kb_id"`
	Kind           string    `bson:"kind"`
	Payload        []byte    `bson:"payload,omitempty"`
	PayloadSchema  string    `bson:"payload_schema,omitempty"`
	CorrelationID  string    `bson:"correlation_id,omitempty"`
	CausationID    string    `bson:"causation_id,omitempty"`
	IdempotencyKey string    `bson:"idempotency_key,omitempty"`
	Status         string    `bson:"status"`
	Attempt        int       `bson:"attempt"`
	MaxAttempts    int       `bson:"max_attempts,omitempty"`
	ClaimedBy      string    `bson:"claimed_by,omitempty"`
	ClaimedUntil   time.Time `bson:"claimed_until,omitempty"`
	CreatedAt      time.Time `bson:"created_at"`
	LastError      string    `bson:"last_error,omitempty"`
}

func toMongoEvent(e Event) mongoEventDoc {
	return mongoEventDoc{
		ID:             e.EventID,
		KBID:           e.KBID,
		Kind:           string(e.Kind),
		Payload:        e.Payload,
		PayloadSchema:  e.PayloadSchema,
		CorrelationID:  e.CorrelationID,
		CausationID:    e.CausationID,
		IdempotencyKey: e.IdempotencyKey,
		Status:         string(e.Status),
		Attempt:        e.Attempt,
		MaxAttempts:    e.MaxAttempts,
		ClaimedBy:      e.ClaimedBy,
		ClaimedUntil:   e.ClaimedUntil,
		CreatedAt:      e.CreatedAt,
		LastError:      e.LastError,
	}
}

func fromMongoEvent(d mongoEventDoc) Event {
	return Event{
		EventID:        d.ID,
		KBID:           d.KBID,
		Kind:           EventKind(d.Kind),
		Payload:        d.Payload,
		PayloadSchema:  d.PayloadSchema,
		CorrelationID:  d.CorrelationID,
		CausationID:    d.CausationID,
		IdempotencyKey: d.IdempotencyKey,
		Status:         EventStatus(d.Status),
		Attempt:        d.Attempt,
		MaxAttempts:    d.MaxAttempts,
		ClaimedBy:      d.ClaimedBy,
		ClaimedUntil:   d.ClaimedUntil,
		CreatedAt:      d.CreatedAt,
		LastError:      d.LastError,
	}
}

// MongoStore is the Mongo-backed Store. When Client is non-nil,
// the store participates in multi-document transactions via InTransaction.
type MongoStore struct {
	Collection *mongo.Collection
	Client     *mongo.Client

	clockMu sync.RWMutex
	clock   Clock
}

// NewMongoStore constructs the store and ensures the unique
// (idempotency_key, kind, kb_id) index exists. Callers own the mongo.Client
// lifecycle. Pass a non-nil client when transactional commits are required
// (they are required for correctness in multi-replica deployments).
func NewMongoStore(ctx context.Context, coll *mongo.Collection, client *mongo.Client) (*MongoStore, error) {
	if coll == nil {
		return nil, errors.New("mongo event store: nil collection")
	}
	s := &MongoStore{Collection: coll, Client: client, clock: RealClock}
	if err := s.ensureIndexes(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// SetClock replaces the store's Clock. Safe for concurrent use.
func (s *MongoStore) SetClock(c Clock) {
	s.clockMu.Lock()
	defer s.clockMu.Unlock()
	if c == nil {
		s.clock = RealClock
		return
	}
	s.clock = c
}

func (s *MongoStore) now() time.Time {
	s.clockMu.RLock()
	defer s.clockMu.RUnlock()
	return nowFrom(s.clock)
}

// InTransaction runs fn inside a Mongo multi-document transaction. Returns
// an error if Client was not wired during construction.
func (s *MongoStore) InTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	if s.Client == nil {
		return errors.New("mongo event store: transactions require a *mongo.Client")
	}
	sess, err := s.Client.StartSession()
	if err != nil {
		return err
	}
	defer sess.EndSession(ctx)
	_, err = sess.WithTransaction(ctx, func(sc context.Context) (any, error) {
		return nil, fn(sc)
	})
	return err
}

func (s *MongoStore) ensureIndexes(ctx context.Context) error {
	partial := bson.D{{Key: "idempotency_key", Value: bson.D{{Key: "$gt", Value: ""}}}}
	idx := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "idempotency_key", Value: 1},
				{Key: mongoKeyKind, Value: 1},
				{Key: mongoKeyKBID, Value: 1},
			},
			Options: options.Index().
				SetUnique(true).
				SetName("uniq_idempotency_kind_kb").
				SetPartialFilterExpression(partial),
		},
		{
			Keys: bson.D{
				{Key: mongoKeyStatus, Value: 1},
				{Key: mongoKeyKind, Value: 1},
				{Key: mongoKeyCreated, Value: 1},
			},
			Options: options.Index().SetName("status_kind_created"),
		},
		{
			Keys:    bson.D{{Key: mongoKeyClaimed, Value: 1}},
			Options: options.Index().SetName(mongoKeyClaimed),
		},
	}
	_, err := s.Collection.Indexes().CreateMany(ctx, idx)
	return err
}

// Append inserts a new event, returning ErrDuplicateKey on idempotency
// collision.
func (s *MongoStore) Append(ctx context.Context, event Event) error {
	if event.EventID == "" {
		return ErrInvalidEvent("event_id required")
	}
	if event.Status == "" {
		event.Status = EventStatusPending
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = s.now()
	}
	if event.MaxAttempts <= 0 {
		event.MaxAttempts = DefaultEventMaxAttempts
	}
	_, err := s.Collection.InsertOne(ctx, toMongoEvent(event))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return ErrDuplicateKey
		}
		return err
	}
	return nil
}

// Claim atomically moves a pending event of the given kind to claimed.
func (s *MongoStore) Claim(
	ctx context.Context,
	kind EventKind,
	workerID string,
	visibility time.Duration,
) (*Event, error) {
	now := s.now()
	filter := bson.M{mongoKeyStatus: string(EventStatusPending), mongoKeyKind: string(kind)}
	update := bson.M{
		"$set": bson.M{
			mongoKeyStatus:  string(EventStatusClaimed),
			"claimed_by":    workerID,
			mongoKeyClaimed: now.Add(visibility),
		},
		"$inc": bson.M{"attempt": 1},
	}
	opts := options.FindOneAndUpdate().
		SetSort(bson.D{{Key: mongoKeyCreated, Value: 1}}).
		SetReturnDocument(options.After)

	var doc mongoEventDoc
	err := s.Collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNoneAvailable
		}
		return nil, err
	}
	out := fromMongoEvent(doc)
	return &out, nil
}

// Ack marks an event Done.
func (s *MongoStore) Ack(ctx context.Context, eventID string) error {
	res, err := s.Collection.UpdateOne(ctx,
		bson.M{mongoKeyID: eventID},
		bson.M{
			"$set":   bson.M{mongoKeyStatus: string(EventStatusDone), "last_error": ""},
			"$unset": bson.M{"payload": ""},
		},
	)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

// Fail transitions the event to pending (or Dead when the stored
// MaxAttempts has been reached) using an aggregation-pipeline update that
// decides the target status server-side. The update is conditional on the
// observed attempt counter: if another worker has already advanced attempt,
// the update returns ErrStateChanged so the caller can re-read.
func (s *MongoStore) Fail(ctx context.Context, eventID string, observedAttempt int, errMsg string) error {
	filter := bson.M{mongoKeyID: eventID}
	if observedAttempt > 0 {
		filter["attempt"] = observedAttempt
	}
	pipeline := bson.A{
		bson.M{
			"$set": bson.M{
				"last_error": errMsg,
				mongoKeyStatus: bson.M{
					"$cond": bson.A{
						bson.M{"$gte": bson.A{
							"$attempt",
							bson.M{"$ifNull": bson.A{"$max_attempts", DefaultEventMaxAttempts}},
						}},
						string(EventStatusDead),
						string(EventStatusPending),
					},
				},
				"claimed_by": bson.M{
					"$cond": bson.A{
						bson.M{"$gte": bson.A{
							"$attempt",
							bson.M{"$ifNull": bson.A{"$max_attempts", DefaultEventMaxAttempts}},
						}},
						"$claimed_by",
						"",
					},
				},
				mongoKeyClaimed: bson.M{
					"$cond": bson.A{
						bson.M{"$gte": bson.A{
							"$attempt",
							bson.M{"$ifNull": bson.A{"$max_attempts", DefaultEventMaxAttempts}},
						}},
						"$claimed_until",
						nil,
					},
				},
				"payload": bson.M{
					"$cond": bson.A{
						bson.M{"$gte": bson.A{
							"$attempt",
							bson.M{"$ifNull": bson.A{"$max_attempts", DefaultEventMaxAttempts}},
						}},
						"$$REMOVE",
						"$payload",
					},
				},
			},
		},
	}
	res, err := s.Collection.UpdateOne(ctx, filter, pipeline)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		if err := s.Collection.FindOne(ctx, bson.M{mongoKeyID: eventID}).Err(); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return ErrNotFound
			}
			return err
		}
		return ErrStateChanged
	}
	return nil
}

// Requeue returns claimed events whose visibility timeout has passed, or
// whose ClaimedUntil is so far in the future it can only be clock skew,
// back to pending.
func (s *MongoStore) Requeue(ctx context.Context, now time.Time) (int, error) {
	if now.IsZero() {
		now = s.now()
	}
	skewCutoff := now.Add(MaxReasonableVisibilityFuture)
	filter := bson.M{
		mongoKeyStatus: string(EventStatusClaimed),
		"$or": bson.A{
			bson.M{mongoKeyClaimed: bson.M{"$lt": now}},
			bson.M{mongoKeyClaimed: bson.M{"$gt": skewCutoff}},
		},
	}
	res, err := s.Collection.UpdateMany(ctx, filter,
		bson.M{
			"$set":   bson.M{mongoKeyStatus: string(EventStatusPending)},
			"$unset": bson.M{"claimed_by": "", mongoKeyClaimed: ""},
		},
	)
	if err != nil {
		return 0, err
	}
	return int(res.ModifiedCount), nil
}

// FindByCausation returns the latest event of the given kind whose
// CausationID equals sourceEventID, or nil when none exists.
func (s *MongoStore) FindByCausation(ctx context.Context, kind EventKind, sourceEventID string) (*Event, error) {
	opts := options.FindOne().SetSort(bson.D{{Key: mongoKeyCreated, Value: -1}, {Key: mongoKeyID, Value: -1}})
	var doc mongoEventDoc
	err := s.Collection.FindOne(ctx,
		bson.M{mongoKeyKind: string(kind), "causation_id": sourceEventID},
		opts,
	).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	out := fromMongoEvent(doc)
	return &out, nil
}

// ListUnfinishedBefore returns up to limit events (default 1000), sorted
// by created_at then _id. A non-empty continuation token means more results
// remain. pass it as `after` to continue.
func (s *MongoStore) ListUnfinishedBefore(
	ctx context.Context,
	kind EventKind,
	before time.Time,
	after string,
	limit int,
) ([]Event, string, error) {
	if limit <= 0 {
		limit = 1000
	}
	filter := bson.M{
		mongoKeyKind:   string(kind),
		mongoKeyStatus: bson.M{"$nin": []string{string(EventStatusDone), string(EventStatusDead)}},
	}
	if !before.IsZero() {
		filter[mongoKeyCreated] = bson.M{"$lt": before}
	}
	if after != "" {
		filter[mongoKeyID] = bson.M{"$gt": after}
	}
	cur, err := s.Collection.Find(ctx, filter,
		options.Find().
			SetSort(bson.D{{Key: mongoKeyCreated, Value: 1}, {Key: mongoKeyID, Value: 1}}).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, "", err
	}
	defer cur.Close(ctx)
	out := make([]Event, 0, limit)
	for cur.Next(ctx) {
		var doc mongoEventDoc
		if err := cur.Decode(&doc); err != nil {
			return nil, "", err
		}
		out = append(out, fromMongoEvent(doc))
	}
	if err := cur.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) == limit {
		next = out[len(out)-1].EventID
	}
	return out, next, nil
}

// Cleanup deletes terminal-state events (done, dead) with created_at older
// than the cutoff. Returns the count removed.
func (s *MongoStore) Cleanup(ctx context.Context, olderThan time.Time) (int, error) {
	if olderThan.IsZero() {
		return 0, nil
	}
	res, err := s.Collection.DeleteMany(ctx, bson.M{
		mongoKeyStatus:  bson.M{"$in": []string{string(EventStatusDone), string(EventStatusDead)}},
		mongoKeyCreated: bson.M{"$lt": olderThan},
	})
	if err != nil {
		return 0, err
	}
	return int(res.DeletedCount), nil
}

// Get returns the event by id.
func (s *MongoStore) Get(ctx context.Context, eventID string) (*Event, error) {
	var doc mongoEventDoc
	err := s.Collection.FindOne(ctx, bson.M{mongoKeyID: eventID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	out := fromMongoEvent(doc)
	return &out, nil
}

// FindByIdempotency returns the first event matching kind/kb/key, or nil when
// none exists.
func (s *MongoStore) FindByIdempotency(
	ctx context.Context,
	kind EventKind,
	kbID, idempotencyKey string,
) (*Event, error) {
	if idempotencyKey == "" {
		return nil, nil
	}
	var doc mongoEventDoc
	err := s.Collection.FindOne(ctx, bson.M{
		mongoKeyKind:      string(kind),
		mongoKeyKBID:      kbID,
		"idempotency_key": idempotencyKey,
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	out := fromMongoEvent(doc)
	return &out, nil
}
