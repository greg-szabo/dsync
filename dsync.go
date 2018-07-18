package main

import (
	"fmt"
	"github.com/greg-szabo/dsync/ddb/sync"
	"strconv"
)

func main() {

	fmt.Println("Use it like the original sync")
	m := sync.Mutex{}
	m.Lock()
	m.Unlock()

	fmt.Println("Keep the value in the DB")
	n := sync.Mutex{}
	n.Lock()
	fmt.Printf("Original value: %s\n", n.Value)
	i, _ := strconv.Atoi(n.Value)
	n.Value = strconv.Itoa(i + 1)
	n.Unlock()

	fmt.Println("Reuse database connectivity")
	o := sync.Mutex{
		AWSSession: n.AWSSession,
		DDBSession: n.DDBSession,
	}
	o.Lock()
	fmt.Printf("Updated value: %s", o.Value)
	o.Unlock()

}
