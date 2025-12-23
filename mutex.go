package mutex

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

type Mutex struct {
	kv
	name string
	stop chan struct{}
}

// NewMutex constructs a distributed mutex. 'root' is the directory where the
// mutex state is stored and unqiuely identifies the mutex. 'name' uniquely
// identifies the client interacting with the mutex. If name is left blank
// then a random name is chosen.
func NewMutex(db fdb.Transactor, root subspace.Subspace, name string) (Mutex, error) {
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
	err := kv.setOwner(db, "")
	if err != nil {
		return Mutex{}, fmt.Errorf("failed to initialize owner key: %w", err)
	}

	return Mutex{
		kv:   kv,
		name: name,
		stop: make(chan struct{}),
	}, nil
}

// AutoRelease runs a loop that checks if the current owner's latest heartbeat is older than the
// specified duration. If so, the owner is assumed to have died and the mutex is released.
// Multiple instances of this function may be run.
func (x *Mutex) AutoRelease(ctx context.Context, db fdb.Database, maxAge time.Duration) error {
	// NOTE: We cannot defer a call to cancel because
	// the variable is reassigned at the end of each
	// loop. We need the newest cancel function to be
	// called before we leave the function, so we must
	// manually call it at every return point.
	childCtx, cancel := context.WithCancel(ctx)
	watch := x.watchOwner(childCtx, db)

	timer := time.NewTimer(maxAge)
	tstamp := time.Now()

	owner, err := x.getOwner(db)
	if err != nil {
		return fmt.Errorf("failed to get owner: %v", err)
	}

	for {
		// Wait for the watch or timer to fire.
		select {
		case err := <-watch:
			if err != nil {
				cancel()
				return fmt.Errorf("failed to wait on watch: %w", err)
			}

		case <-timer.C:
		}

		// Check the age of the heartbeat and release the mutex if necessary.
		ret, err := db.Transact(func(tr fdb.Transaction) (any, error) {
			curOwner, err := x.getOwner(tr)
			if err != nil {
				return nil, fmt.Errorf("failed to get owner: %w", err)
			}

			// If the owner changed, the heartbeat was updated,
			// or the heartbeat isn't old enough, return the
			// current owner without releasing the mutex.
			switch {
			case owner.name != curOwner.name:
				fallthrough
			case !bytes.Equal(owner.hbeat, curOwner.hbeat):
				fallthrough
			case time.Since(tstamp) < maxAge:
				return curOwner, nil
			}

			// The owner hasn't sent a heartbeat in a while.
			// Assume they are dead and release the lock.
			name, err := x.dequeue(tr)
			if err != nil {
				return nil, fmt.Errorf("failed to dequeue: %w", err)
			}
			err = x.setOwner(tr, name)
			if err != nil {
				return nil, fmt.Errorf("failed to set owner: %w", err)
			}
			return ownerKV{name: name}, nil
		})
		if err != nil {
			cancel()
			return err
		}

		curOwner := ret.(ownerKV)

		// If the owner or heartbeat was updated, then
		// store the new ownerKV and reset the timer.
		switch {
		case owner.name != curOwner.name:
			fallthrough
		case !bytes.Equal(owner.hbeat, curOwner.hbeat):
			tstamp = time.Now()
			timer.Reset(maxAge)
			owner = curOwner
		}

		// Cancel the current watch and create a new one.
		// This ensures we are watching the latest owner KV
		// in case the owner has changed during this cycle.
		cancel()
		childCtx, cancel = context.WithCancel(ctx)
		watch = x.watchOwner(childCtx, db)
	}
}

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
			case <-x.stop:
				return

			case <-ticker.C:
				_ = x.heartbeat(db, x.name)
			}
		}
	}()
}

func (x *Mutex) stopBeating() {
	x.stop <- struct{}{}
}
