package reader

import (
	"context"
	"log"
	"time"

	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoReader struct {
	client *mongo.Client
	cursor *mongo.Cursor
	ctx    context.Context
	cancel context.CancelFunc
}

func NewMongoReader(connectionString string) (*MongoReader, error) {
	// Create a context with timeout for the connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connectionString))
	if err != nil {
		cancel()
		log.Printf("unable to connect to mongo: %v", err)
		return nil, err
	}

	// Ping the database to verify connection
	err = client.Ping(ctx, nil)
	if err != nil {
		cancel()
		client.Disconnect(ctx)
		log.Printf("unable to ping mongo: %v", err)
		return nil, err
	}

	// Create a new context for operations
	opCtx, opCancel := context.WithCancel(context.Background())

	return &MongoReader{
		client: client,
		ctx:    opCtx,
		cancel: opCancel,
	}, nil
}

func (s *MongoReader) ReadOplog() (model.Oplog, error) {
	// Initialize cursor if it doesn't exist
	if s.cursor == nil {
		// Access the oplog collection
		oplogCollection := s.client.Database("local").Collection("oplog.rs")

		// Create options for tailing the oplog
		opts := options.Find().
			SetCursorType(options.TailableAwait).
			SetMaxAwaitTime(1 * time.Second).
			SetBatchSize(1)

		// Query the oplog, sorting by timestamp
		cursor, err := oplogCollection.Find(s.ctx, bson.M{}, opts)
		if err != nil {
			return model.Oplog{}, err
		}

		s.cursor = cursor
	}

	// Try to get the next document
	if s.cursor.Next(s.ctx) {
		var oplog model.Oplog
		if err := s.cursor.Decode(&oplog); err != nil {
			return model.Oplog{}, err
		}
		return oplog, nil
	}

	// Check if there was an error
	if err := s.cursor.Err(); err != nil {
		return model.Oplog{}, err
	}

	// Return an empty oplog with nil error to indicate no more entries for now
	return model.Oplog{}, nil
}

func (s *MongoReader) Close() {
	if s.cursor != nil {
		s.cursor.Close(s.ctx)
	}

	if s.client != nil {
		s.client.Disconnect(s.ctx)
	}

	s.cancel()
}
