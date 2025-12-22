package mutex

import (
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type Mutex struct {
	name  string
	root  subspace.Subspace
	done  chan struct{}
}

// NewMutex constructs a distributed mutex. 'root' is the directory where the
// mutex state is stored and unqiuely identifies the mutex. 'name' uniquely
// identifies the client interacting with the mutex.
func NewMutex(db fdb.Transactor, root subspace.Subspace, name string) Mutex {
	return Mutex{
		name:  name,
		root:  root,
		done:  make(chan struct{}, 1),
	}
}

func (x *Mutex) TryAcquire(db fdb.Database) (bool, error) {
	acquired, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		owner, _, err := x.getOwner(tr)
		if err != nil {
			return nil, err
		}

		switch owner {
		case x.name:
			return true, nil

		case "":
			err := x.setOwner(tr, x.name)
			if err != nil {
				return nil, err
			}
			return true, nil

		default:
			return false, x.enqueue(db, x.name)
		}
	})
	if err != nil {
		return false, err
	}

	if acquired.(bool) {
		x.startBeating(db)
		return true, nil
	}
	return false, nil
}

func (x *Mutex) Release(db fdb.Transactor) error {
	_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		name, _, err := x.getOwner(tr)
		if err != nil {
			return nil, err
		}

		if x.name != name {
			return nil, nil
		}

		name, err = x.dequeue(tr)
		if err != nil {
			return nil, err
		}

		return nil, x.setOwner(tr, name)
	})
	if err != nil {
		return err
	}

	x.stopBeating()
	return nil
}

func (x *Mutex) startBeating(db fdb.Database) {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-x.done:
				return

			case <-ticker.C:
				_ = x.heartbeat(db, x.name)
			}
		}
	}()
}

func (x *Mutex) stopBeating() {
	x.done <- struct{}{}
}

func (x *Mutex) setOwner(db fdb.Transactor, name string) error {
	rngOwner, err := x.packOwnerRange()
	if err != nil {
		return err
	}

	_, err = db.Transact(func(tr fdb.Transaction) (any, error) {
		tr.ClearRange(rngOwner)
		tr.Set(x.packOwnerKey(name), nil)
		return nil, nil
	})
	return err
}

// getOwner returns the name and heartbeat of the client currently holding the mutex.
func (x *Mutex) getOwner(db fdb.Transactor) (string, []byte, error) {
	type Owner struct {
		name  string
		hbeat []byte
	}

	rngRoot, err := x.packOwnerRange()
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
		name, err := x.unpackOwnerKey(kv.Key)
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

func (x *Mutex) heartbeat(db fdb.Transactor, name string) error {
	if name == "" {
		return nil
	}

	_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		curName, _, err := x.getOwner(db)
		if err != nil {
			return nil, err
		}

		// If we're not the owner, don't heartbeat.
		if name != curName {
			return nil, nil
		}

		// Update the heartbeat using the current versionstamp.
		tr.SetVersionstampedValue(x.packOwnerKey(name), x.packOwnerValue())
		return nil, nil
	})
	return err
}

// enqueue places the client in the queue for control of the mutex.
func (x *Mutex) enqueue(db fdb.Transactor, name string) error {
	rngQueue, err := x.packQueueRange()
	if err != nil {
		return err
	}

	_, err = db.Transact(func(tr fdb.Transaction) (any, error) {
		iter := tr.GetRange(rngQueue, fdb.RangeOptions{}).Iterator()

		// If we're already enqueued, skip this operation.
		for iter.Advance() {
			if name == x.unpackQueueValue(iter.MustGet().Value) {
				return nil, nil
			}
		}

		key, err := x.packQueueKey()
		if err != nil {
			return nil, fmt.Errorf("failed to pack the queue key: %w", err)
		}

		// Place ourselves at the end of the queue.
		tr.SetVersionstampedKey(key, x.packQueueValue(name))
		return nil, nil
	})
	return err
}

// dequeue pops the name off the front of the queue and returns it.
func (x *Mutex) dequeue(db fdb.Transactor) (string, error) {
	rng, err := x.packQueueRange()
	if err != nil {
		return "", err
	}

	name, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		iter := tr.GetRange(rng, fdb.RangeOptions{Limit: 1}).Iterator()
		if !iter.Advance() {
			return "", nil
		}

		kv := iter.MustGet()
		tr.Clear(kv.Key)
		return x.unpackQueueValue(kv.Value), nil
	})
	if err != nil {
		return "", err
	}
	return name.(string), nil
}

// Some of the methods below don't include
// much logic. Their primary purpose is to
// define the KV schema. All prefixes, keys,
// and values are constructed by calling
// these methods.

func (x *Mutex) packOwnerRange() (fdb.KeyRange, error) {
	return fdb.PrefixRange(x.root.Pack(tuple.Tuple{"owner"}))
}

func (x *Mutex) packOwnerKey(name string) fdb.Key {
	return x.root.Pack(tuple.Tuple{"owner", name})
}

func (x *Mutex) unpackOwnerKey(key fdb.Key) (string, error) {
	tup, err := x.root.Unpack(key)
	if err != nil {
		return "", fmt.Errorf("failed to unpack tuple: %w", err)
	}
	if len(tup) != 2 {
		return "", fmt.Errorf("tuple is incorrect length %d", len(tup))
	}
	// The 1st element should be the string "owner". We won't
	// bother confirming that. The 2nd is the name of the owner.
	name, ok := tup[1].(string)
	if !ok {
		return "", fmt.Errorf("tuple element 1 is not a string")
	}
	return name, nil
}

func (x *Mutex) packOwnerValue() []byte {
	// Return a blank parameter for versionstamping
	// the value. This will result in the value
	// simply being the 12 byte versionstamp.
	// See [[fdb.Transaction.SetVersionstampedValue]]
	// for details.
	return make([]byte, 16)
}

func (x *Mutex) packQueueRange() (fdb.KeyRange, error) {
	return fdb.PrefixRange(x.root.Pack(tuple.Tuple{"queue"}))
}

func (x *Mutex) packQueueKey() (fdb.Key, error) {
	tup := tuple.Tuple{"queue", tuple.IncompleteVersionstamp(0)}
	return tup.PackWithVersionstamp(x.root.Bytes())
}

func (x *Mutex) packQueueValue(name string) []byte {
	return []byte(name)
}

func (x *Mutex) unpackQueueValue(val []byte) string {
	return string(val)
}
