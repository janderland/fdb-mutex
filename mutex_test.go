package mutex

import (
	"crypto/rand"
	"encoding/hex"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/stretchr/testify/require"
)

func TestPrivateMethods(t *testing.T) {
	tests := map[string]testFn{
		"empty": func(t *testing.T, db fdb.Transactor, root directory.DirectorySubspace) {
			x, err := NewMutex(db, root, "client")
			require.NoError(t, err)

			err = x.dequeue(db)
			require.NoError(t, err)

			err = x.heartbeat(db)
			require.NoError(t, err)

			name, hbeat, err := x.owner(db)
			require.NoError(t, err)
			require.Equal(t, name, "")
			require.Empty(t, hbeat)
		},
		"locked": func(t *testing.T, db fdb.Transactor, root directory.DirectorySubspace) {
			x, err := NewMutex(db, root, "client")
			require.NoError(t, err)

			err = x.enqueue(db)
			require.NoError(t, err)

			err = x.dequeue(db)
			require.NoError(t, err)

			name, hbeat, err := x.owner(db)
			require.NoError(t, err)
			require.Equal(t, name, "client")
			require.Empty(t, hbeat)
		},
		"heartbeat": func(t *testing.T, db fdb.Transactor, root directory.DirectorySubspace) {
			x, err := NewMutex(db, root, "client")
			require.NoError(t, err)

			err = x.enqueue(db)
			require.NoError(t, err)

			err = x.dequeue(db)
			require.NoError(t, err)

			err = x.heartbeat(db)
			require.NoError(t, err)

			name, hbeat, err := x.owner(db)
			require.NoError(t, err)
			require.Equal(t, name, "client")
			require.NotEmpty(t, hbeat)
		},
		"non-owner heartbeat": func(t *testing.T, db fdb.Transactor, root directory.DirectorySubspace) {
			x1, err := NewMutex(db, root, "client1")
			require.NoError(t, err)

			x2, err := NewMutex(db, root, "client2")
			require.NoError(t, err)

			err = x1.enqueue(db)
			require.NoError(t, err)

			err = x2.enqueue(db)
			require.NoError(t, err)

			err = x2.dequeue(db)
			require.NoError(t, err)

			err = x2.heartbeat(db)
			require.NoError(t, err)

			name, hbeat, err := x2.owner(db)
			require.NoError(t, err)
			require.Equal(t, name, "client1")
			require.Empty(t, hbeat)
		},
	}

	runTests(t, tests)
}

type testFn func(t *testing.T, db fdb.Transactor, root directory.DirectorySubspace)

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
