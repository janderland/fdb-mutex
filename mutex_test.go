package mutex

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/stretchr/testify/require"
)

func TestKV(t *testing.T) {
	tests := map[string]testFn{
		"empty": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x := kv{root}

			name, err := x.dequeue(db)
			require.NoError(t, err)
			require.Empty(t, name)

			err = x.heartbeat(db, "")
			require.NoError(t, err)

			owner, err := x.getOwner(db)
			require.NoError(t, err)
			require.Equal(t, "", owner.name)
			require.Empty(t, owner.hbeat)
		},
		"queue": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x := kv{root}

			err := x.enqueue(db, "clientZ")
			require.NoError(t, err)

			err = x.enqueue(db, "clientA")
			require.NoError(t, err)

			name, err := x.dequeue(db)
			require.NoError(t, err)
			require.Equal(t, "clientZ", name)
		},
		"owner": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x := kv{root}

			err := x.setOwner(db, "client")
			require.NoError(t, err)

			owner, err := x.getOwner(db)
			require.NoError(t, err)
			require.Equal(t, "client", owner.name)
			require.Empty(t, owner.hbeat)
		},
		"heartbeat": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x := kv{root}

			err := x.setOwner(db, "client")
			require.NoError(t, err)

			err = x.heartbeat(db, "client")
			require.NoError(t, err)

			owner, err := x.getOwner(db)
			require.NoError(t, err)
			require.NotEmpty(t, owner.hbeat)
		},
		"non-owner heartbeat": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x := kv{root}

			err := x.setOwner(db, "clientA")
			require.NoError(t, err)

			err = x.heartbeat(db, "clientZ")
			require.NoError(t, err)

			owner, err := x.getOwner(db)
			require.NoError(t, err)
			require.Empty(t, owner.hbeat)
		},
		"watch owner": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x := kv{root}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err := x.setOwner(db, "clientA")
			require.NoError(t, err)

			watch := x.watchOwner(ctx, db)

			err = x.setOwner(db, "clientB")
			require.NoError(t, err)

			require.NoError(t, <-watch)
		},
		"cancel watch": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x := kv{root}

			ctx, cancel := context.WithCancel(context.Background())
			watch := x.watchOwner(ctx, db)

			cancel()
			require.Error(t, <-watch)
		},
	}

	runTests(t, tests)
}

func TestAcquire(t *testing.T) {
	tests := map[string]testFn{
		"non-blocking": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x1, err := NewMutex(db, root, "")
			require.NoError(t, err)

			x2, err := NewMutex(db, root, "")
			require.NoError(t, err)

			acquired, err := x1.TryAcquire(db)
			require.NoError(t, err)
			require.True(t, acquired)

			acquired, err = x2.TryAcquire(db)
			require.NoError(t, err)
			require.False(t, acquired)

			err = x1.Release(db)
			require.NoError(t, err)

			acquired, err = x2.TryAcquire(db)
			require.NoError(t, err)
			require.True(t, acquired)
		},
		"blocking": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x1, err := NewMutex(db, root, "client1")
			require.NoError(t, err)

			x2, err := NewMutex(db, root, "client2")
			require.NoError(t, err)

			err = x1.Acquire(context.Background(), db)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			defer cancel()

			go func() {
				<-ctx.Done()
				if err := x1.Release(db); err != nil {
					t.Errorf("failed to release: %v", err)
				}
			}()

			err = x2.Acquire(context.Background(), db)
			require.NoError(t, err)

			owner, err := x2.getOwner(db)
			require.NoError(t, err)
			require.Equal(t, owner.name, "client2")
		},
		"heartbeat": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x, err := NewMutex(db, root, "")
			require.NoError(t, err)

			_, err = x.TryAcquire(db)
			require.NoError(t, err)

			// Wait for the heartbeat to update.
			<-x.watchOwner(context.Background(), db)

			owner, err := x.getOwner(db)
			require.NoError(t, err)
			require.NotEmpty(t, owner.hbeat)
		},
	}

	runTests(t, tests)
}

func TestAutoRelease(t *testing.T) {
	tests := map[string]testFn {
		"empty": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x, err := NewMutex(db, root, "client")
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			goAutoRelease(t, x, ctx, db, 500*time.Millisecond)

			acquired, err := x.TryAcquire(db)
			require.NoError(t, err)
			require.True(t, acquired)

			// Stop heartbeating so auto release is triggered.
			x.stopBeating()

			// Wait for owner to be auto-released.
			<-x.watchOwner(context.Background(), db)

			owner, err := x.getOwner(db)
			require.NoError(t, err)
			require.Empty(t, owner.name)
			require.Empty(t, owner.hbeat)
		},
		"acquired": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x, err := NewMutex(db, root, "client")
			require.NoError(t, err)

			acquired, err := x.TryAcquire(db)
			require.NoError(t, err)
			require.True(t, acquired)

			// Stop heartbeating so auto release is triggered.
			x.stopBeating()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			goAutoRelease(t, x, ctx, db, 500*time.Millisecond)

			// Wait for owner to be auto-released.
			<-x.watchOwner(context.Background(), db)

			owner, err := x.getOwner(db)
			require.NoError(t, err)
			require.Empty(t, owner.name)
			require.Empty(t, owner.hbeat)
		},
		"heartbeat": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
		},
	}

	runTests(t, tests)
}

type testFn func(t *testing.T, db fdb.Database, root subspace.Subspace)

func runTests(t *testing.T, tests map[string]testFn) {
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runTest(t, test)
		})
	}
}

func runTest(t *testing.T, test testFn) {
	fdb.MustAPIVersion(730)
	db := fdb.MustOpenDefault()

	// Generate a random directory name.
	randBytes := make([]byte, 8)
	if _, err := rand.Read(randBytes); err != nil {
		t.Fatalf("failed to generate random bytes: %v", err)
	}
	dirName := hex.EncodeToString(randBytes)

	root, err := directory.CreateOrOpen(db, []string{dirName}, nil)
	if err != nil {
		t.Fatalf("failed to create root directory: %v", err)
	}

	defer func() {
		if _, err := directory.Root().Remove(db, []string{dirName}); err != nil {
			t.Errorf("failed to delete root directory: %v", err)
		}
	}()

	test(t, db, root)
}

func goAutoRelease(t *testing.T, x Mutex, ctx context.Context, db fdb.Database, maxAge time.Duration) {
	go func() {
		err := x.AutoRelease(ctx, db, maxAge)
		if err != nil {
			var ferr fdb.Error
			if errors.As(err, &ferr) && ferr.Code == 1101 {
				// Ignore "operation cancelled" errors.
				return
			}
			t.Errorf("auto release exited: %v", err)
		}
	}()
}
