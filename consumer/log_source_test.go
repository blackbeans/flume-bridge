package consumer

import (
	"flume-log-sdk/config"
	"flume-log-sdk/consumer/pool"
	"github.com/blackbeans/redigo/redis"
	"strconv"
	"testing"
	"time"
)

const (
	LOG = "{\"action\":\"location\",\"params\":{\"lat\":39.996176990443,\"lng\":116.47478272276,\"acc\":65,\"ip\":\"106.39.7.226\",\"loctype\":0,\"timestamp\":1404447385,\"body\":{\"lat\":39.996176990443,\"lng\":116.47478272276,\"acc\":65,\"ip\":\"106.39.7.226\",\"loctype\":0,\"timestamp\":1404447385},\"momoid\":\"100777\",\"businessName\":\"location\",\"type\":\"update\"}}"
)

func Test_SourceServer(t *testing.T) {

	hp := config.HostPort{Host: "localhost", Port: 44444}
	err, poollink := pool.NewFlumePoolLink(hp)
	if nil != err {
		t.Fail()
	}
	flumepools := []*pool.FlumePoolLink{poollink}

	v := config.HostPort{Host: "localhost", Port: 6379}

	pool := redis.NewPool(func() (conn redis.Conn, err error) {

		conn, err = redis.DialTimeout("tcp", v.Host+":"+strconv.Itoa(v.Port),
			time.Duration(5)*time.Second,
			time.Duration(5)*time.Second,
			time.Duration(5)*time.Second)

		return
	}, time.Duration(5*2)*time.Second, 10/2, 10)

	redisPools := make(map[string][]*redis.Pool)
	redisPools["new-log"] = []*redis.Pool{pool}

	sourceserver := newSourceServer("location", redisPools, flumepools)

	go func() { sourceserver.start() }()

	for i := 0; i < 100; i++ {
		sourceserver.testPushLog("new-log", LOG)
		sourceserver.testPushLog("new-log", LOG)
		sourceserver.testPushLog("new-log", LOG)
		sourceserver.testPushLog("new-log", LOG)
		// sourceserver.testPushLog("new-log", LOG)
		// sourceserver.testPushLog("new-log", LOG)
		// sourceserver.testPushLog("new-log", LOG)
		// sourceserver.testPushLog("new-log", LOG)

	}
	time.Sleep(10 * time.Second)
	sourceserver.stop()
}
