package main

import (
	"flag"
	"flume-bridge/config"
	"flume-bridge/consumer"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {

	runtime.GOMAXPROCS(8)

	baseLog := flag.String("logpath", "/home/logs/flume-log", "basic log path ")
	instancename := flag.String("instancename", "flume-log", "instance name ")
	queuename := flag.String("queuename", "user-log", "config queuename ")
	redisHost := flag.String("redis", "redis_node_6008:6008", "redishost")
	redisconns := flag.Int("redis-maxconn", 20, "config redis max connetions")

	zkhost := flag.String("zkhost", "momo-zk-001.m6:2210,momo-zk-002.m6:2210,momo-zk-003.m6:2210", "zkhost")
	business := flag.String("businesses", "location", " businesses")
	pprofPort := flag.Int("pport", -1, "pprof port default value is -1 ")
	isCompress := flag.Bool("iscompress", false, "is compress")
	flag.Parse()

	go func() {
		if *pprofPort > 0 {
			log.Println(http.ListenAndServe(":"+strconv.Itoa(*pprofPort), nil))
		}
	}()

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
	option := config.NewOption(*baseLog, businessArr, *zkhost, queueHosts, *isCompress)
	sourcemanager := consumer.NewSourceManager(*instancename, option)

	sourcemanager.Start()

	log.Println("FLUME_LOG|CMD|[kill -30 $pid dump heap !]")

	//接受系统信号量
	var s = make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGKILL, syscall.SIGUSR1)
	//是否收到kill的命令
	for {
		cmd := <-s
		if cmd == syscall.SIGKILL {
			break
		} else if cmd == syscall.SIGUSR1 {
			//如果为siguser1则进行dump内存
			unixtime := time.Now().Unix()
			path := *baseLog + "/" + *instancename + "/heapdump-" + *instancename + fmt.Sprintf("%d", unixtime)
			f, err := os.Create(path)
			if nil != err {
				log.Println("FLUME_LOG|ERROR|DUMP HEAP|" + err.Error())
				continue
			} else {
				debug.WriteHeapDump(f.Fd())
				log.Println("FLUME_LOG|SUCC|DUMP HEAP|PATH:%s" + path)
			}
		} else {
			log.Println("FLUME_LOG|NO SIGN REG|" + cmd.String())
		}
	}
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
