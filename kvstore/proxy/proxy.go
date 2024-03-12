package main

import "sync"

/*
	Proxy between kvserver and kvclient
	Responsible for forwarding requests to reasonable nodes
	and deciding whether to merge historical write requests from each node (Writeless)
*/

type Proxy struct {
	KVservers []string
	Address   string

	// "key": 3, ...
	putCountsInProxy sync.Map
	// "key": 5, ...
	predictPutCounts sync.Map
	// "key": 3, ...
	getCountsInTotal sync.Map
	// "key": 3, ...
	putCountsInTotal sync.Map
}

func main() {

}
