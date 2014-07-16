package consumer

import (
	"testing"
)

const (
	LOG = "{\"action\":\"location\",\"params\":{\"lat\":39.996176990443,\"lng\":116.47478272276,\"acc\":65,\"ip\":\"106.39.7.226\",\"loctype\":0,\"timestamp\":1404447385,\"body\":{\"lat\":39.996176990443,\"lng\":116.47478272276,\"acc\":65,\"ip\":\"106.39.7.226\",\"loctype\":0,\"timestamp\":1404447385},\"momoid\":\"100777\",\"businessName\":\"location\",\"type\":\"update\"}}"
)

func Test_SinkServer(t *testing.T) {

	flumeAgents := []HostPort{HostPort{Host: "localhost", Port: 44444}}

	qhost := QueueHostPort{QueueName: "new-log", Maxconn: 20, Timeout: 5}
	qhost.HostPort = HostPort{Host: "localhost", Port: 6379}

	hostPorts := []QueueHostPort{qhost}

	option := NewOption(flumeAgents, hostPorts)
	sinkserver := NewSinkServer(option)

	go func() { sinkserver.Start() }()

	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)
	sinkserver.testPushLog("new-log", LOG)

	sinkserver.Stop()
}
