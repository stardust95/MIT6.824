package raft

import (
	"log"
	"math/rand"
)
// Debug : Debugging
const Debug = 0

// DPrintf :
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Random :
func Random(min int, max int) int {
	return rand.Intn(max - min) + min
}