package config

type Log struct {
	Option string
	Key    string
	Value  string
}

// Address for KV Service Between Server and Client
var Address string = "192.168.10.129:3088"

// Address for Internal Communication Between Nodes
var InternalAddress string = "192.168.10.129:30881"
