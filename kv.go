package mutex

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

type ownerKV struct {
	name  string
	hbeat []byte
}

// kv implements the various queries performed by [[Mutex]]. Some
// of the methods of kv don't include much logic but explicitly
// define the DB schema.
type kv struct{ subspace.Subspace }

// setOwner sets the owner key for the client with the provided name.
func (x *kv) setOwner(db fdb.Transactor, name string) error {
	rngOwner, err := x.packOwnerRange()
	if err != nil {
		return err
	}

	_, err = db.Transact(func(tr fdb.Transaction) (any, error) {
		// Clear any existing owner keys.
		tr.ClearRange(rngOwner)

		// Set the owner key. The heartbeat (value) is left
		// empty. It's set by the [[kv.heartbeat]] method.
		tr.Set(x.packOwnerKey(name), nil)
		return nil, nil
	})
	return err
}

// getOwner returns the name and heartbeat of the client currently holding the mutex.
func (x *kv) getOwner(db fdb.Transactor) (ownerKV, error) {
	rngRoot, err := x.packOwnerRange()
	if err != nil {
		return ownerKV{}, err
	}

	owner, err := db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		// There should only be 1 owner, so range read that single KV.
		iter := tr.GetRange(rngRoot, fdb.RangeOptions{Limit: 1}).Iterator()
		if !iter.Advance() {
			return ownerKV{}, nil
		}

		kv := iter.MustGet()
		name, err := x.unpackOwnerKey(kv.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to unpack root key: %w", err)
		}

		return ownerKV{
			name:  name,
			hbeat: kv.Value,
		}, nil
	})
	if err != nil {
		return ownerKV{}, err
	}
	return owner.(ownerKV), nil
}

// watchOwner returns a channel which signals an ownership change. When the owner
// changes, the channel returns nil. If the watch setup fails or the provided context
// is canceled, the channel retuns an error.
func (x *kv) watchOwner(ctx context.Context, db fdb.Transactor) <-chan error {
	ch := make(chan error, 1)

	ret, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		owner, err := x.getOwner(tr)
		if err != nil {
			return nil, err
		}
		return tr.Watch(x.packOwnerKey(owner.name)), nil
	})
	if err != nil {
		ch <- err
		return ch
	}

	watch := ret.(fdb.FutureNil)

	go func() {
		<-ctx.Done()
		watch.Cancel()
	}()

	go func() {
		ch <- watch.Get()
	}()

	return ch
}

// heartbeat updates the heartbeat for the client with the provided name.
// If the provided name doesn't belong to the owner of the mutex then this
// method is a noop.
func (x *kv) heartbeat(db fdb.Transactor, name string) error {
	if name == "" {
		return nil
	}

	_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		owner, err := x.getOwner(db)
		if err != nil {
			return nil, err
		}

		// If we're not the owner, don't heartbeat.
		if name != owner.name {
			return nil, nil
		}

		// Update the heartbeat using the current versionstamp.
		tr.SetVersionstampedValue(x.packOwnerKey(name), x.packOwnerValue())
		return nil, nil
	})
	return err
}

// enqueue places the provided client in the queue for control of the mutex.
// If the provided name is already in the queue then this method is a noop.
func (x *kv) enqueue(db fdb.Transactor, name string) error {
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
func (x *kv) dequeue(db fdb.Transactor) (string, error) {
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

func (x *kv) packOwnerRange() (fdb.KeyRange, error) {
	return fdb.PrefixRange(x.Pack(tuple.Tuple{"owner"}))
}

func (x *kv) packOwnerKey(name string) fdb.Key {
	return x.Pack(tuple.Tuple{"owner", name})
}

func (x *kv) unpackOwnerKey(key fdb.Key) (string, error) {
	tup, err := x.Unpack(key)
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

func (x *kv) packOwnerValue() []byte {
	// Return a blank parameter for versionstamping
	// the value. This will result in the value
	// simply being the 12 byte versionstamp.
	// See [[fdb.Transaction.SetVersionstampedValue]]
	// for details.
	return make([]byte, 16)
}

func (x *kv) packQueueRange() (fdb.KeyRange, error) {
	return fdb.PrefixRange(x.Pack(tuple.Tuple{"queue"}))
}

func (x *kv) packQueueKey() (fdb.Key, error) {
	tup := tuple.Tuple{"queue", tuple.IncompleteVersionstamp(0)}
	return tup.PackWithVersionstamp(x.Bytes())
}

func (x *kv) packQueueValue(name string) []byte {
	return []byte(name)
}

func (x *kv) unpackQueueValue(val []byte) string {
	return string(val)
}
