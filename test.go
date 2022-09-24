package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	m := sync.Mutex{}
	c := sync.NewCond(&m)
	go func() {
		time.Sleep(1 * time.Second)
		fmt.Printf("Broadcast!\n")
		c.Broadcast()
		// m.Unlock()
	}()
	m.Lock()
	// time.Sleep(2 * time.Second)
	c.Wait()
	fmt.Printf("wake up!\n")
	m.Unlock()
}
