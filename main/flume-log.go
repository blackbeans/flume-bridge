package main

import (
	"flag"
	"flume-log-sdk/config"
	"flume-log-sdk/consumer"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
)

func main() {

	runtime.GOMAXPROCS(4)
	baseLog := flag.String("logpath", "/home/logs/flume-log", "basic log path ")
	instancename := flag.String("instancename", "flume-log", "instance name ")
	queuename := flag.String("queuename", "user-log", "config queuename ")
	redisHost := flag.String("redis", "redis_node_6008:6008", "redishost")
	redisconns := flag.Int("redis-maxconn", 20, "config redis max connetions")

	zkhost := flag.String("zkhost", "momo-zk-001.m6:2210,momo-zk-002.m6:2210,momo-zk-003.m6:2210", "zkhost")
	business := flag.String("businesses", "location", " businesses")
	flag.Parse()

	maxconn := *redisconns
	maxIdelTime := 5

	log.Printf("queuename:%s,redis:%s,flume:%s\n", *queuename, *redisHost, *zkhost)
	queueHosts := make([]config.QueueHostPort, 0)
	for _, hp := range parseHostPort(*redisHost) {
		qhost := config.QueueHostPort{QueueName: *queuename, Maxconn: maxconn, Timeout: maxIdelTime}
		qhost.HostPort = hp
		queueHosts = append(queueHosts, qhost)
	}

	businessArr := strings.Split(*business, ",")
	option := config.NewOption(*baseLog, businessArr, *zkhost, queueHosts)
	sourcemanager := consumer.NewSourceManager(*instancename, option)

	sourcemanager.Start()

	// reload server list
	var s = make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGKILL)
	//是否收到kill的命令
	<-s
	sourcemanager.Close()
	log.Printf("FLUME_LOG|STOPPED|%s", *instancename)
}

func parseHostPort(hps string) []config.HostPort {
	hostports := make([]config.HostPort, 0)
	for _, v := range strings.Split(hps, ",") {
		hostports = append(hostports, config.NewHostPort(v))
	}

	return hostports
}
