package consumer

import (
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"log-sink/rpc/flume"
	"net"
	"os"
	"strconv"
)

type Status int32

const (
	STATUS_INIT  Status = 0
	STATUS_READY Status = 1
	STATUS_DEAD  Status = 2
)

type flumeClient struct {
	host      string
	port      int
	transport *thrift.TFramedTransport
	client    *flume.ThriftSourceProtocolClient
	status    Status //连接状态
}

func newFlumeClient(host string, port int) *flumeClient {

	return &flumeClient{host: host, port: port, status: STATUS_INIT}

}

func (self *flumeClient) isAlive() bool {
	return self.status == STATUS_READY

}

func (self *flumeClient) connect() {

	//创建一个物理连接
	tsocket, err := thrift.NewTSocket(net.JoinHostPort(self.host, strconv.Itoa(self.port)))

	if nil != err {
		log.Panic(err)
		os.Exit(-1)
	}

	self.transport = thrift.NewTFramedTransport(tsocket)

	protoFactory := thrift.NewTCompactProtocolFactory()
	self.client = flume.NewThriftSourceProtocolClientFactory(self.transport, protoFactory)

	if err := self.transport.Open(); nil != err {
		log.Panic(err)
		os.Exit(-1)
	}

	self.status = STATUS_READY
}

func (self *flumeClient) append(event *flume.ThriftFlumeEvent) error {

	ch := make(chan flume.Status, 1)
	go func(ch chan flume.Status) {
		//获取远程调用的对象
		status, _ := self.client.Append(event)
		ch <- status

	}(ch)

	defer close(ch)
	stauts := <-ch

	//如果没有成功则向上抛出
	if stauts != flume.Status_OK {
		return errors.New("deliver fail !")
	}
	return nil

}

func (self *flumeClient) destory() {

	self.status = STATUS_DEAD
	err := self.transport.Close()

	log.Panicln(err)
}
