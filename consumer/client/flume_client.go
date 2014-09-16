package client

import (
	"errors"
	"flume-log-sdk/rpc/flume"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"net"
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
	host             string
	port             int
	tsocket          *thrift.TSocket
	transport        thrift.TTransport
	transportFactory thrift.TTransportFactory
	protocolFactory  *thrift.TCompactProtocolFactory

	thriftclient *flume.ThriftSourceProtocolClient
	status       Status //连接状态
}

func NewFlumeClient(host string, port int) *FlumeClient {

	return &FlumeClient{host: host, port: port, status: STATUS_INIT}

}

func (self *FlumeClient) IsAlive() bool {
	return self.status == STATUS_READY

}

func (self *FlumeClient) Connect() error {

	var tsocket *thrift.TSocket
	var err error
	//创建一个物理连接
	tsocket, err = thrift.NewTSocketTimeout(net.JoinHostPort(self.host, strconv.Itoa(self.port)), 10*time.Second)
	if nil != err {
		log.Printf("FLUME_CLIENT|CREATE TSOCKET|FAIL|%s|%s\n", self.HostPort(), err)
		return err
	}

	self.tsocket = tsocket
	self.transportFactory = thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	//TLV 方式传输
	self.protocolFactory = thrift.NewTCompactProtocolFactory()

	self.clientConn()
	self.status = STATUS_READY
	go self.checkAlive()

	return nil
}

func (self *FlumeClient) clientConn() error {
	//使用非阻塞io来传输
	self.transport = self.transportFactory.GetTransport(self.tsocket)
	self.thriftclient = flume.NewThriftSourceProtocolClientFactory(self.transport, self.protocolFactory)
	if err := self.transport.Open(); nil != err {
		log.Printf("FLUME_CLIENT|CREATE THRIFT CLIENT|FAIL|%s|%s", self.HostPort(), err)
		return err
	}
	return nil
}

func (self *FlumeClient) checkAlive() {
	for self.status != STATUS_DEAD {
		//休息1s
		time.Sleep(1 * time.Second)
		isOpen := self.tsocket.IsOpen()
		if !isOpen {
			self.status = STATUS_DEAD
			log.Printf("flume : %s:%d is Dead", self.host, self.port)
			break
		}

	}
}

func (self *FlumeClient) AppendBatch(events []*flume.ThriftFlumeEvent) error {
	return self.innerSend(func() (flume.Status, error) {
		return self.thriftclient.AppendBatch(events)
	})
}

func (self *FlumeClient) Append(event *flume.ThriftFlumeEvent) error {

	return self.innerSend(func() (flume.Status, error) {
		return self.thriftclient.Append(event)
	})
}

func (self *FlumeClient) innerSend(sendfunc func() (flume.Status, error)) error {

	if self.status == STATUS_DEAD {
		return errors.New("FLUME_CLIENT|DEAD|" + self.HostPort())
	}

	//如果transport关闭了那么久重新打开
	if !self.transport.IsOpen() {
		//重新建立thriftclient
		err := self.clientConn()
		if nil != err {
			log.Printf("FLUME_CLIENT|SEND EVENT|CLIENT CONN | CREATE FAIL|%s\n", err.Error())
			return err
		}
	}

	status, err := sendfunc()
	if nil != err {
		log.Printf("FLUME_CLIENT|SEND EVENT|FAIL|%s|STATUS:%s\n", err.Error(), status)
		status = flume.Status_ERROR
		self.status = STATUS_DEAD
	}

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
		log.Println(err.Error())
	}

}

func NewFlumeEvent(business, action string, body []byte) *flume.ThriftFlumeEvent {
	//拼装头部信息
	header := make(map[string]string, 2)
	header["businessName"] = business
	header["type"] = action
	event := flume.NewThriftFlumeEvent()
	event.Headers = header
	event.Body = body

	return event
}

func (self *FlumeClient) HostPort() string {
	return fmt.Sprintf("[%s:%d-%s]", self.host, self.port, self.status)
}
