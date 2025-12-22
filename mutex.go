package mutex

import (
	// "bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type Mutex struct {
	kv
	name string
	done chan struct{}
}

// NewMutex constructs a distributed mutex. 'root' is the directory where the
// mutex state is stored and unqiuely identifies the mutex. 'name' uniquely
// identifies the client interacting with the mutex. If name is left blank
// then a random name is chosen.
func NewMutex(db fdb.Transactor, root subspace.Subspace, name string) Mutex {
	if name == "" {
		var randBytes [32]byte
		if _, err := rand.Read(randBytes[:]); err != nil {
			panic(fmt.Errorf("failed to generate a random name: %w", err))
		}
		name = hex.EncodeToString(randBytes[:])
	}

	kv := kv{root}

	// Set a blank owner to initialize the owner key.
	// This allows kv.watchOwner() to trigger on the
	// first acquire.
	// TODO: handle the error.
	_ = kv.setOwner(db, "")

	return Mutex{
		kv:   kv,
		name: name,
		done: make(chan struct{}, 1),
	}
}

/*
// AutoRelease runs a loop that checks if the current owner's latest heartbeat is older than the specified duration.
// If so, the owner is assumed to have died and the mutex is released. Multiple instances of this function may be run.
func AutoRelease(ctx context.Context, db fdb.Database, root subspace.Subspace, maxAge time.Duration) error {
	kv := kv{root}

	// Initial setup for watch and timer. These two
	// will be reinitialized at the end of each loop.
	//
	// NOTE: We cannot defer a call to cancel because
	// the variable is reassigned at the end of each
	// loop. We need the latest assigned value to be
	// called before we leave the function, so we must
	// manually call it at every return point.
	childCtx, cancel := context.WithCancel(ctx)
	watch := kv.watchOwner(childCtx, db)
	timer := time.NewTimer(maxAge)

	var (
		tstamp time.Time
		hbeat  [12]byte
		name   string
	)

	for {
		// Wait for the watch or timer to fire.
		select {
		case err := <-watch:
			if err != nil {
				cancel()
				return fmt.Errorf("failed to wait on watch", err)
			}

		case <-timer.C:
		}

		// Check the age of the heartbeat and release the mutex if necessary.
		_, err := db.Transact(func(t fdb.Transaction) (interface{}, error) {
			curName, curHbeat, err := kv.getOwner(db)
			if err != nil {
				return nil, err
			}

			if name != curName {
				name = curName
				copy(hbeat[:], curHbeat)
				tstamp = time.Now()
				return nil, nil
			}

			if bytes.Compare(hbeat[:], curHbeat) != 0 {
				copy(hbeat[:], curHbeat)
				tstamp = time.Now()
				return nil, nil
			}

			if dur := time.Now().Sub(tstamp); dur > maxAge {

			}
			return nil, nil
		})
		if err != nil {
			cancel()
			return fmt.Errorf("failed to handle watch trigger: %w", err)
		}

		// Reset the watch and timer.
		cancel()
		childCtx, cancel = context.WithCancel(ctx)
		watch = kv.watchOwner(childCtx, db)
		_ = timer.Reset(maxAge)
	}
}
*/

func (x *Mutex) TryAcquire(db fdb.Database) (bool, error) {
	acquired, err := db.Transact(func(tr fdb.Transaction) (any, error) {
		owner, err := x.getOwner(tr)
		if err != nil {
			return nil, err
		}

		switch owner.name {
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
		owner, err := x.getOwner(tr)
		if err != nil {
			return nil, err
		}

		if x.name != owner.name {
			return nil, nil
		}

		name, err := x.dequeue(tr)
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

type kv struct{ subspace.Subspace }

func (x *kv) setOwner(db fdb.Transactor, name string) error {
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

type ownerHeartbeat struct {
	name  string
	hbeat []byte
}

// getOwner returns the name and heartbeat of the client currently holding the mutex.
func (x *kv) getOwner(db fdb.Transactor) (ownerHeartbeat, error) {
	rngRoot, err := x.packOwnerRange()
	if err != nil {
		return ownerHeartbeat{}, err
	}

	owner, err := db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		// There should only be 1 owner, so range read that single KV.
		iter := tr.GetRange(rngRoot, fdb.RangeOptions{Limit: 1}).Iterator()
		if !iter.Advance() {
			return ownerHeartbeat{}, nil
		}

		kv := iter.MustGet()
		name, err := x.unpackOwnerKey(kv.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to unpack root key: %w", err)
		}

		return ownerHeartbeat{
			name:  name,
			hbeat: kv.Value,
		}, nil
	})
	if err != nil {
		return ownerHeartbeat{}, err
	}
	return owner.(ownerHeartbeat), nil
}

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

// enqueue places the client in the queue for control of the mutex.
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

// Some of the methods below don't include much logic. Their primary
// purpose is to define the KV schema. All prefixes, keys, and values
// are constructed by calling these methods.

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
