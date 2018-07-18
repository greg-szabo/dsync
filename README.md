# dsync

## Overview
dsync is a sync library for distributed Go processes.

It works similarly to the built-in sync library, only it synchronizes across multiple processes - even on different machines.

This is achieved by storing the semaphore state and the value on a remote database.

Currently only DynamoDB is implemented. This might change in the future

## Prerequisites

1. Install dep from [here](https://github.com/golang/dep/releases).
2. Run `make dep_vendor_deps`
3. Set up your AWS API keys (or IAM role)

## How to use

- Run `make build` or `make test`

OR

- import "github.com/greg-szabo/dsync/ddb/sync"

## Acknowledgements

Shout out to [Ryan Smith](https://github.com/ryandotsmith/) who created [ddbsync](https://github.com/ryandotsmith/ddbsync) which has a similar concept. Among others, his work inspired this library.
