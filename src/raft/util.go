package raft

import "log"

// Debug : Debugging
const Debug = 1

// DPrintf :
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
