package main

import (
	"flag"
	. "flume-log-sdk/consumer"
	"strings"
)

func main() {

	queuename := flag.String("queuename", "user-log", "config queuename ")
	redisHost := flag.String("redis", "redis_node_6008:6008", "redishost")
	maxconn := 10
	maxIdelTime := 5

	flumeAgent := flag.String("flume", "flume001.m6:61111,flume002.m6:61112", "flumehost")

	queueHosts := make([]QueueHostPort, 0)
	for _, hp := range parseHostPort(*redisHost) {
		qhost := QueueHostPort{QueueName: *queuename, Maxconn: maxconn, Timeout: maxIdelTime}
		qhost.HostPort = hp
		queueHosts = append(queueHosts, qhost)
	}

	flumeAgents := parseHostPort(*flumeAgent)

	option := NewOption(flumeAgents, queueHosts)
	sinkserver := NewSinkServer(option)

	sinkserver.Start()

}

func parseHostPort(hps string) []HostPort {
	hostports := make([]HostPort, 0)
	for _, v := range strings.Split(hps, ",") {
		hostports = append(hostports, NewHostPort(v))
	}

	return hostports
}
