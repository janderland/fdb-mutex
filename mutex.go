package mutex

import (
	"encoding/binary"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// Forward the Tuple type into the current namespace.
type Tuple = tuple.Tuple

type Mutex struct {
	name  string
	root  subspace.Subspace
	queue subspace.Subspace

	// TODO: Scan the queue instead of using an index.
	index subspace.Subspace
}

// NewMutex constructs a distributed mutex. 'path' is the directory path where
// the mutex state is stored and unqiuely identifies the mutex. 'name' uniquely
// identifies the client interacting with the mutex.
func NewMutex(db fdb.Transactor, path []string, name string) (*Mutex, error) {
	root, err := directory.Open(db, path, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open root dir", err)
	}

	queue, err := root.Open(db, []string{"queue"}, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open queue dir", err)
	}

	index, err := root.Open(db, []string{"index"}, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open index dir", err)
	}

	x := &Mutex{
		name:  name,
		root:  root,
		queue: queue,
		index: index,
	}

	// Start the IDs at 1.
	_, err = x.getAndIncID(db)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to init next ID", err)
	}

	return x, nil
}

func (x *Mutex) TryAcquire(db fdb.Transactor) (bool, error) {
	panic("not implemented")
}

func (x *Mutex) Release(db fdb.Transactor) error {
	panic("not implemented")
}

// getAndIncID returns the next queue ID before incrementing the ID.
func (x *Mutex) getAndIncID(db fdb.Transactor) (uint64, error) {
	key := x.root.Pack(Tuple{"next_id"})

	id, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		tr.Add(key, []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
		val := tr.Get(key).MustGet()
		if val == nil {
			return 0, nil
		}
		return binary.LittleEndian.Uint64(val), nil
	})
	if err != nil {
		return 0, err
	}
	return id.(uint64), nil
}

// getHolder returns the name of the client currently holding the mutex.
func (x *Mutex) getHolder(db fdb.Transactor) (string, error) {
	key := x.root.Pack(Tuple{"holder"})

	name, err := db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		// TODO: Use value for heartbeat, return name & heartbeat.
		return string(tr.Get(key).MustGet()), nil
	})
	if err != nil {
		return "", err
	}
	return name.(string), nil
}

func (x *Mutex) enqueue(db fdb.Transactor) error {
	_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		id, err := x.getAndIncID(tr)
		if err != nil {
			return nil, err
		}

		keyQueue := x.queue.Pack(Tuple{id})
		keyIndex := x.index.Pack(Tuple{x.name})

		tr.Set(keyQueue, nil)
		tr.Set(keyIndex, []byte(x.name))
		return nil, nil
	})
	return err
}

func (x *Mutex) dequeue(db fdb.Transactor) error {
	rngQueue, err := fdb.PrefixRange(x.queue.Bytes())
	if err != nil {
		return err
	}

	rngHolder, err := fdb.PrefixRange(x.root.Pack(Tuple{"holder"}))
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

		keyIndex := x.index.Pack(Tuple{nextName})
		keyHolder := x.root.Pack(Tuple{"holder", nextName})

		tr.ClearRange(rngHolder)
		tr.Set(keyHolder, nil)
		tr.Clear(kvQueue.Key)
		tr.Clear(keyIndex)
		return nil, nil
	})
	return err
}
