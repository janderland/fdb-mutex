package mutex

import (
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/stretchr/testify/require"
)

func TestPrivateMethods(t *testing.T) {
	tests := map[string]testFn{
		"empty": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x := NewMutex(db, root, "")

			name, err := x.dequeue(db)
			require.NoError(t, err)
			require.Empty(t, name)

			err = x.heartbeat(db, "")
			require.NoError(t, err)

			name, hbeat, err := x.getOwner(db)
			require.NoError(t, err)
			require.Equal(t, "", name)
			require.Empty(t, hbeat)
		},
		"queue": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x := NewMutex(db, root, "")

			err := x.enqueue(db, "clientZ")
			require.NoError(t, err)

			err = x.enqueue(db, "clientA")
			require.NoError(t, err)

			name, err := x.dequeue(db)
			require.NoError(t, err)
			require.Equal(t, "clientZ", name)
		},
		"owner": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x := NewMutex(db, root, "")

			err := x.setOwner(db, "client")
			require.NoError(t, err)

			name, hbeat, err := x.getOwner(db)
			require.NoError(t, err)
			require.Equal(t, "client", name)
			require.Empty(t, hbeat)
		},
		"heartbeat": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x := NewMutex(db, root, "")

			err := x.setOwner(db, "client")
			require.NoError(t, err)

			err = x.heartbeat(db, "client")
			require.NoError(t, err)

			_, hbeat, err := x.getOwner(db)
			require.NoError(t, err)
			require.NotEmpty(t, hbeat)
		},
		"non-owner heartbeat": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x := NewMutex(db, root, "")

			err := x.setOwner(db, "clientA")
			require.NoError(t, err)

			err = x.heartbeat(db, "clientZ")
			require.NoError(t, err)

			_, hbeat, err := x.getOwner(db)
			require.NoError(t, err)
			require.Empty(t, hbeat)
		},
	}

	runTests(t, tests)
}

func TestTryAcquire(t *testing.T) {
	tests := map[string]testFn{
		"locked": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x1 := NewMutex(db, root, "")
			x2 := NewMutex(db, root, "")

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
		"heartbeat": func(t *testing.T, db fdb.Database, root subspace.Subspace) {
			x := NewMutex(db, root, "")

			_, err := x.TryAcquire(db)
			require.NoError(t, err)

			// Wait for goroutine to update heartbeat.
			time.Sleep(1500 * time.Millisecond)

			_, hbeat, err := x.getOwner(db)
			require.NoError(t, err)
			require.NotEmpty(t, hbeat)
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
