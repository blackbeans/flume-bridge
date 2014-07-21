package main

import (
	"flag"
	"flume-log-sdk/config"
	"flume-log-sdk/consumer"
	"log"
	"strings"
)

func main() {

	queuename := flag.String("queuename", "user-log", "config queuename ")
	redisHost := flag.String("redis", "redis_node_6008:6008", "redishost")
	maxconn := 10
	maxIdelTime := 5

	flumeAgent := flag.String("flume", "flume001.m6:61111,flume002.m6:61112", "flumehost")

	flag.Parse()

	log.Printf("queuename:%s,redis:%s,flume:%s\n", *queuename, *redisHost, *flumeAgent)
	queueHosts := make([]config.QueueHostPort, 0)
	for _, hp := range parseHostPort(*redisHost) {
		qhost := config.QueueHostPort{QueueName: *queuename, Maxconn: maxconn, Timeout: maxIdelTime}
		qhost.HostPort = hp
		queueHosts = append(queueHosts, qhost)
	}

	flumeAgents := parseHostPort(*flumeAgent)

	option := config.NewOption(flumeAgents, queueHosts)
	sinkserver := consumer.NewSinkServer(option)

	sinkserver.Start()

}

func parseHostPort(hps string) []config.HostPort {
	hostports := make([]config.HostPort, 0)
	for _, v := range strings.Split(hps, ",") {
		hostports = append(hostports, config.NewHostPort(v))
	}

	return hostports
}
