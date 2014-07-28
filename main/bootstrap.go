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

	zkhost := flag.String("zkhost", "momo-zk-001.m6:2181,momo-zk-002.m6:2181,momo-zk-003.m6:2181", "zkhost")
	business := flag.String("businesses", "location", " businesses")
	flag.Parse()

	log.Printf("queuename:%s,redis:%s,flume:%s\n", *queuename, *redisHost, *zkhost)
	queueHosts := make([]config.QueueHostPort, 0)
	for _, hp := range parseHostPort(*redisHost) {
		qhost := config.QueueHostPort{QueueName: *queuename, Maxconn: maxconn, Timeout: maxIdelTime}
		qhost.HostPort = hp
		queueHosts = append(queueHosts, qhost)
	}

	businessArr := strings.Split(*business, ",")
	option := config.NewOption(businessArr, *zkhost, queueHosts)
	sinkmanager := consumer.NewSinkManager(option)

	sinkmanager.Start()

}

func parseHostPort(hps string) []config.HostPort {
	hostports := make([]config.HostPort, 0)
	for _, v := range strings.Split(hps, ",") {
		hostports = append(hostports, config.NewHostPort(v))
	}

	return hostports
}
