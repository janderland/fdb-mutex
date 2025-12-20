package mutex

type Mutex {
	dir directory.Directory
}

func NewMutex(path []string) (*Mutex, error) {
}

func (x *Mutex) Acquire() (func(), error) {
}
