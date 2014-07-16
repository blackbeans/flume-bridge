package client

import (
	"errors"
	"flume-log-sdk/rpc/flume"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

type Status int32

const (
	STATUS_INIT  Status = 0
	STATUS_READY Status = 1
	STATUS_DEAD  Status = 2
)

type FlumeClient struct {
	host         string
	port         int
	transport    thrift.TTransport
	thriftclient *flume.ThriftSourceProtocolClient
	status       Status //连接状态
}

func NewFlumeClient(host string, port int) *FlumeClient {

	return &FlumeClient{host: host, port: port, status: STATUS_INIT}

}

func (self *FlumeClient) IsAlive() bool {
	return self.status == STATUS_READY

}

func (self *FlumeClient) Connect() {

	//创建一个物理连接
	tsocket, err := thrift.NewTSocket(net.JoinHostPort(self.host, strconv.Itoa(self.port)))

	if nil != err {
		log.Panic(err)
		os.Exit(-1)
	}

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())

	//TLV 方式传输
	protocolFactory := thrift.NewTCompactProtocolFactory()

	//使用非阻塞io来传输
	self.transport = transportFactory.GetTransport(tsocket)

	self.thriftclient = flume.NewThriftSourceProtocolClientFactory(self.transport, protocolFactory)

	if err := self.transport.Open(); nil != err {
		log.Panic(err)
		os.Exit(-1)
	}

	self.status = STATUS_READY

	go self.checkAlive()
}

func (self *FlumeClient) checkAlive() {
	for self.status != STATUS_DEAD {
		//休息1s
		time.Sleep(1 * time.Second)
		isOpen := self.thriftclient.Transport.IsOpen()
		if !isOpen {
			self.status = STATUS_DEAD
			log.Printf("flume : %s:%d is Dead", self.host, self.port)
			break
		}

	}
}

func (self *FlumeClient) Append(header map[string]string, body []byte) error {

	if self.status == STATUS_DEAD {
		return errors.New("flume client is dead")
	}

	var err error
	event := flume.NewThriftFlumeEvent()
	event.Headers = header
	event.Body = body

	if nil != err {
		return err
	}
	ch := make(chan flume.Status, 1)

	go func(ch chan flume.Status) {

		//获取远程调用的对象
		status, err := self.thriftclient.Append(event)

		if nil != err {
			log.Println("send flume event fail " + err.Error())
			status = flume.Status_ERROR
		}
		ch <- status

	}(ch)

	defer close(ch)
	status := <-ch

	//如果没有成功则向上抛出
	if status != flume.Status_OK {
		return errors.New("deliver fail ! " + status.String())
	}
	return nil

}

func (self *FlumeClient) Destroy() {

	self.status = STATUS_DEAD
	err := self.transport.Close()

	if nil != err {
		log.Panicln(err.Error())
	}

}
