package consumer

import (
	"testing"
	"time"
)

func Test_SinkServer(t *testing.T) {

	flumeAgents := []HostPort{HostPort{Host: "localhost", Port: 44444}}

	qhost := QueueHostPort{QueueName: "new-log", Maxconn: 20, Timeout: 5}
	qhost.HostPort = HostPort{Host: "localhost", Port: 6379}

	hostPorts := []QueueHostPort{qhost}

	option := NewOption(flumeAgents, hostPorts)
	sinkserver := NewSinkServer(option)

	sinkserver.Start()

	// sinkserver.stop()

	time.Sleep(60 * 10000000)

	sinkserver.Stop()
}
