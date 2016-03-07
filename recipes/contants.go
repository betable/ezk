package recipes

import "errors"

var (
	ErrLockFound = errors.New("lock found")
	ErrNotLocked = errors.New("not locked")
)
