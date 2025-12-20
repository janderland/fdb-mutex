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

// NewMutex constructs a distributed mutex. 'path' is the directory path where
// the mutex state is stored and unqiuely identifies the mutex. 'name' uniquely
// identifies the client interacting with the mutex.
func NewMutex(db fdb.Transactor, path []string, name string) (*Mutex, error) {
	root, err := directory.Open(db, path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open root dir: %w", err)
	}

	queue, err := root.Open(db, []string{"queue"}, nil)
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

// getOwner returns the name and heartbeat of the client currently holding the mutex.
func (x *Mutex) getOwner(db fdb.Transactor) (string, []byte, error) {
	type Owner struct {
		name  string
		hbeat []byte
	}

	rngRoot, err := fdb.PrefixRange(x.root.Bytes())
	if err != nil {
		return "", nil, err
	}

	owner, err := db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		iter := tr.GetRange(rngRoot, fdb.RangeOptions{Limit: 1}).Iterator()
		if !iter.Advance() {
			return nil, nil
		}

		kv := iter.MustGet()
		tup, err := tuple.Unpack(kv.Key)
		if err != nil {
			return nil, err
		}

		return Owner{
			name:  tup[0].(string),
			hbeat: kv.Value,
		}, nil
	})
	if err != nil {
		return "", nil, err
	}
	o := owner.(Owner)
	return o.name, o.hbeat, nil
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
			if x.name == string(iter.MustGet().Value) {
				return nil, nil
			}
		}

		tup := tuple.Tuple{tuple.IncompleteVersionstamp(0)}
		key, err := tup.PackWithVersionstamp(x.queue.Bytes())
		if err != nil {
			return nil, err
		}
		tr.Set(fdb.Key(key), []byte(x.name))
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
		nextName := string(kvQueue.Value)
		keyRoot := x.root.Pack(tuple.Tuple{nextName})

		tr.ClearRange(rngRoot)
		tr.Set(keyRoot, nil)
		tr.Clear(kvQueue.Key)
		return nil, nil
	})
	return err
}
