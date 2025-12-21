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
	runTest(t, func(t *testing.T, db fdb.Transactor, root directory.DirectorySubspace) {
		x, err := NewMutex(db, root, "client")
		require.NoError(t, err)

		err = x.dequeue(db)
		require.NoError(t, err)

		name, hbeat, err := x.owner(db)
		require.NoError(t, err)
		require.Zero(t, name)
		require.Empty(t, hbeat)

		err = x.enqueue(db)
		require.NoError(t, err)

		err = x.dequeue(db)
		require.NoError(t, err)

		name, hbeat, err = x.owner(db)
		require.NoError(t, err)
		require.Equal(t, name, "client")
		require.Empty(t, hbeat)

		err = x.heartbeat(db)
		require.NoError(t, err)

		name, hbeat, err = x.owner(db)
		require.NoError(t, err)
		require.Equal(t, name, "client")
		require.NotEmpty(t, hbeat)
	})
}

func runTest(t *testing.T, fn func(t *testing.T, db fdb.Transactor, root directory.DirectorySubspace)) {
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

	fn(t, db, root)
}
