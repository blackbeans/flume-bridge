package consumer

import (
	"errors"
	"flume-log-sdk/rpc/flume"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
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
	host             string
	port             int
	bodyProtoFactory *thrift.TCompactProtocolFactory
	transport        *thrift.TFramedTransport
	client           *flume.ThriftSourceProtocolClient
	status           Status //连接状态
}

func NewFlumeClient(host string, port int) *flumeClient {

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
	protocolFactory := thrift.NewTCompactProtocolFactory()
	self.client = flume.NewThriftSourceProtocolClientFactory(self.transport, protocolFactory)

	//body 序列化方式

	self.bodyProtoFactory = thrift.NewTCompactProtocolFactory()

	if err := self.transport.Open(); nil != err {
		log.Panic(err)
		os.Exit(-1)
	}

	self.status = STATUS_READY
}

func (self *flumeClient) append(header map[string]string, body string) error {

	buff := thrift.NewTMemoryBufferLen(len(body))

	defer buff.Close()

	_, err := buff.WriteString(body)
	if nil != err {
		return errors.New("body convert to byte array fail")
	}

	event := flume.NewThriftFlumeEvent()
	event.Headers = header
	event.Body = buff.Bytes()
	// protocol := self.bodyProtoFactory.GetProtocol(buff)
	// err = event.Write(protocol)
	if nil != err {
		return errors.New("body serilized body fail |" + err.Error())
	}

	ch := make(chan flume.Status, 1)

	go func(ch chan flume.Status) {

		//获取远程调用的对象
		status, err := self.client.Append(event)

		if nil != err {
			log.Println("send flume event fail " + err.Error())
		}
		ch <- status

	}(ch)

	defer close(ch)
	status := <-ch

	//如果没有成功则向上抛出
	if status != flume.Status_OK {
		return errors.New("deliver fail !")
	}
	return nil

}

func (self *flumeClient) destory() {

	self.status = STATUS_DEAD
	err := self.transport.Close()

	if nil != err {
		log.Panicln(err.Error())
	}

}
