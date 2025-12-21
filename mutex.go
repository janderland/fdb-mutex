package mutex

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type Mutex struct {
	name  string
	root  subspace.Subspace
	queue subspace.Subspace
}

// NewMutex constructs a distributed mutex. 'root' is the directory where the
// mutex state is stored and unqiuely identifies the mutex. 'name' uniquely
// identifies the client interacting with the mutex.
func NewMutex(db fdb.Transactor, root directory.DirectorySubspace, name string) (*Mutex, error) {
	queue, err := root.CreateOrOpen(db, []string{"queue"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open queue dir: %w", err)
	}

	return &Mutex{
		name:  name,
		root:  root,
		queue: queue,
	}, nil
}

func (x *Mutex) TryAcquire(db fdb.Transactor) (bool, error) {
	panic("not implemented")
}

func (x *Mutex) Release(db fdb.Transactor) error {
	panic("not implemented")
}

// owner returns the name and heartbeat of the client currently holding the mutex.
func (x *Mutex) owner(db fdb.Transactor) (string, []byte, error) {
	type Owner struct {
		name  string
		hbeat []byte
	}

	rngRoot, err := fdb.PrefixRange(x.root.Bytes())
	if err != nil {
		return "", nil, err
	}

	owner, err := db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		// There should only be 1 owner, so range read that single KV.
		iter := tr.GetRange(rngRoot, fdb.RangeOptions{Limit: 1}).Iterator()
		if !iter.Advance() {
			return Owner{}, nil
		}

		kv := iter.MustGet()
		name, err := x.unpackRootKey(kv.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to unpack root key: %w", err)
		}

		return Owner{
			name:  name,
			hbeat: kv.Value,
		}, nil
	})
	if err != nil {
		return "", nil, err
	}
	o := owner.(Owner)
	return o.name, o.hbeat, nil
}

func (x *Mutex) heartbeat(db fdb.Transactor) error {
	_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		name, _, err := x.owner(db)
		if err != nil {
			return nil, err
		}

		// If we're not the owner, don't heartbeat.
		if name != x.name {
			return nil, nil
		}

		// Update the heartbeat using the current versionstamp.
		tr.SetVersionstampedValue(x.packRootKey(x.name), x.packRootValue())
		return nil, nil
	})
	return err
}

// enqueue places the client in the queue for control of the mutex.
func (x *Mutex) enqueue(db fdb.Transactor) error {
	rngQueue, err := fdb.PrefixRange(x.queue.Bytes())
	if err != nil {
		return err
	}

	_, err = db.Transact(func(tr fdb.Transaction) (any, error) {
		iter := tr.GetRange(rngQueue, fdb.RangeOptions{}).Iterator()

		// If we're already enqueued, skip this operation.
		for iter.Advance() {
			if x.name == x.unpackQueueValue(iter.MustGet().Value) {
				return nil, nil
			}
		}

		key, err := x.packQueueKey()
		if err != nil {
			return nil, fmt.Errorf("failed to pack the queue key: %w", err)
		}

		// Place ourselves at the end of the queue.
		tr.SetVersionstampedKey(key, x.packQueueValue())
		return nil, nil
	})
	return err
}

// dequeue pops a client off the front of the queue
// and gives them control of the mutex.
func (x *Mutex) dequeue(db fdb.Transactor) error {
	rngQueue, err := fdb.PrefixRange(x.queue.Bytes())
	if err != nil {
		return err
	}

	rngRoot, err := fdb.PrefixRange(x.root.Bytes())
	if err != nil {
		return err
	}

	_, err = db.Transact(func(tr fdb.Transaction) (any, error) {
		iterQueue := tr.GetRange(rngQueue, fdb.RangeOptions{Limit: 1}).Iterator()
		if !iterQueue.Advance() {
			return nil, nil
		}

		kvQueue := iterQueue.MustGet()
		nextName := x.unpackQueueValue(kvQueue.Value)
		keyRoot := x.packRootKey(nextName)

		// Clear any owner keys.
		tr.ClearRange(rngRoot)

		// Set the new owner key.
		tr.Set(keyRoot, nil)

		// Remove the head of the queue.
		tr.Clear(kvQueue.Key)
		return nil, nil
	})
	return err
}

// Some of the methods below don't include
// much logic. Their primary purpose is to
// define the KV schema. All keys & values
// are constructed by calling these methods.

func (x *Mutex) packRootKey(name string) fdb.Key {
	return x.root.Pack(tuple.Tuple{name})
}

func (x *Mutex) unpackRootKey(key fdb.Key) (string, error) {
	tup, err := x.root.Unpack(key)
	if err != nil {
		return "", fmt.Errorf("failed to unpack tuple: %w", err)
	}
	if len(tup) != 1 {
		return "", fmt.Errorf("tuple is incorrect length %d", len(tup))
	}
	name, ok := tup[0].(string)
	if !ok {
		return "", fmt.Errorf("tuple element 1 is not a string")
	}
	return name, nil
}

func (x *Mutex) packRootValue() []byte {
	// Return a blank parameter for versionstamping
	// the value. This will result in the value
	// simply being the 12 byte versionstamp.
	return make([]byte, 16)
}	

func (x *Mutex) packQueueKey() (fdb.Key, error) {
	tup := tuple.Tuple{tuple.IncompleteVersionstamp(0)}
	return tup.PackWithVersionstamp(x.queue.Bytes())
}

func (x *Mutex) packQueueValue() []byte {
	return []byte(x.name)
}

func (x *Mutex) unpackQueueValue(val []byte) string {
	return string(val)
}
