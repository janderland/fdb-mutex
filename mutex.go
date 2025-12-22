package mutex

import (
	// "bytes"
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

	// stops background heartbeats
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
