package consumer

import (
	"flume-log-sdk/config"
	"flume-log-sdk/consumer/client"
	"github.com/blackbeans/redigo/redis"
	"strconv"
	"testing"
	"time"
)

const (
	LOG = "{\"action\":\"location\",\"params\":{\"lat\":39.996176990443,\"lng\":116.47478272276,\"acc\":65,\"ip\":\"106.39.7.226\",\"loctype\":0,\"timestamp\":1404447385,\"body\":{\"lat\":39.996176990443,\"lng\":116.47478272276,\"acc\":65,\"ip\":\"106.39.7.226\",\"loctype\":0,\"timestamp\":1404447385},\"momoid\":\"100777\",\"businessName\":\"location\",\"type\":\"update\"}}"
)

func Test_SinkServer(t *testing.T) {

	hp := config.HostPort{Host: "localhost", Port: 44444}

	flumePool := newFlumeClientPool(20, 50, 100, 10*time.Second, func() *client.FlumeClient {
		flumeclient := client.NewFlumeClient(hp.Host, hp.Port)
		flumeclient.Connect()
		return flumeclient
	})

	flumepools := []*flumeClientPool{flumePool}

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

	sinkserver := newSinkServer(redisPools, flumepools)

	go func() { sinkserver.start() }()

	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)

	sinkserver.stop()
}
