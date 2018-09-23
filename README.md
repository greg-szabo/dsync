# dsync

## Overview
dsync is a sync library for distributed Go processes.

It works similarly to the built-in sync library, only it synchronizes across multiple processes - even on different machines.

This is achieved by storing the semaphore state and the value on a remote database.

Currently only DynamoDB is implemented. This might change in the future (Redis is considered currently).

## Prerequisites

- DB access for the chosen implementation

## How to use

```bash
$ export AWS_ACCESS_KEY=access
$ export AWS_SECRET_KEY=secret
```

```go
// ./main.go

package main

import "github.com/greg-szabo/dsync/ddb/sync"

func main() {
		m := sync.Mutex{}
		m.Lock()
		defer m.Unlock()
		// do important work here
		return
}
```

```bash
$ go get github.com/greg-szabo/dsync/ddb/sync
$ go run main.go
```

The locking mechanism will automatically create a `Locks` database in DynamoDB and store the Mutex details there.


## API Documentation

- [DynamoDB implementation](https://godoc.org/github.com/greg-szabo/dsync/ddb/sync)
- [dsync interface](https://godoc.org/github.com/greg-szabo/dsync/dsync)

## Acknowledgements

Shout out to [Ryan Smith](https://github.com/ryandotsmith/) who created [ddbsync](https://github.com/ryandotsmith/ddbsync) which has a similar concept. Among others, his work inspired this library.
