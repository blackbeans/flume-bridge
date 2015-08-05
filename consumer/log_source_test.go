package consumer

import (
	"container/list"
	"flume-bridge/config"
	"flume-bridge/consumer/pool"
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

	list := list.New()
	list.PushFront(poollink)
	sourceserver := newSourceServer("location", list)

	go func() { sourceserver.start() }()

	for i := 0; i < 100; i++ {
		_, message := decodeCommand([]byte(LOG))
		sourceserver.buffChannel <- message
	}
	time.Sleep(10 * time.Second)
	sourceserver.stop()
}
