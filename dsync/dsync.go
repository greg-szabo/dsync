// Package dsync provides primitives for distributed synchronization.
// This package is usually not used by itself.
package dsync

// A Locker represents an object that stores a value that can be locked and unlocked.
type Locker interface {
	Lock()
	Unlock()
	GetValueInt64() int64
	GetValueUint64() uint64
	SetValueInt64(value int64)
	SetValueUint64(value uint64)
	GetValueString() string
	SetValueString(value string)
}
