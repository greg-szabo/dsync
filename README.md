# dsync

## Overview
dsync is a sync library for distributed Go processes.

It works similarly to the built-in sync library, only it synchronizes across multiple processes - even on different machines.

This is achieved by storing the semaphore state and the value on a remote database.

Currently only DynamoDB is implemented. This might change in the future (Redis is considered currently).

## Prerequisites

- DB access for the chosen implementation

## Documentation

- [DynamoDB implementation](https://golang.org/pkg/greg-szabo/dsync/ddb/sync)
- [dsync interface](https://golang.org/pkg/greg-szabo/dsync/dsync)

## Acknowledgements

Shout out to [Ryan Smith](https://github.com/ryandotsmith/) who created [ddbsync](https://github.com/ryandotsmith/ddbsync) which has a similar concept. Among others, his work inspired this library.