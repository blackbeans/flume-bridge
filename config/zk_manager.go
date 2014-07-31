package config

import (
	"github.com/blackbeans/zk"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	FLUME_PATH = "/flume"
)

//配置的flumenode
type FlumeNode struct {
	BusinessName string     `json:'business'`
	Flume        []HostPort `json:'flume'`
}

type ZKManager struct {
	session *zk.Session
}

type ZkEvent zk.EventType

const (
	Created ZkEvent = 1 // From Exists, Get
	Deleted ZkEvent = 2 // From Exists, Get
	Changed ZkEvent = 3 // From Exists, Get
	Child   ZkEvent = 4 // From Children
)

func NewZKManager(zkhosts string) *ZKManager {
	if len(zkhosts) <= 0 {
		log.Println("使用默认zkhosts！|localhost:2181\n")
		zkhosts = "localhost:2181"
	} else {
		log.Printf("使用zkhosts:[%s]！\n", zkhosts)
	}

	conf := &zk.Config{Addrs: strings.Split(zkhosts, ","), Timeout: 5 * time.Second}

	ss, err := zk.Dial(conf)
	if nil != err {
		panic("连接zk失败..." + err.Error())
		return nil
	}

	exist, _, err := ss.Exists(FLUME_PATH, nil)
	if nil != err {
		panic("无法创建flume_path " + err.Error())
	}

	if !exist {

		resp, err := ss.Create(FLUME_PATH, nil, zk.CreatePersistent, zk.AclOpen)
		if nil != err {
			panic("can't create flume root path ! " + err.Error())
		} else {
			log.Println("create flume root path succ ! " + resp)
		}
	}

	return &ZKManager{session: ss}
}

func (self *ZKManager) GetAndWatch(businsess string, existWatcher func(path string, eventType ZkEvent),
	childWatcher func(path string, childNode []HostPort)) *FlumeNode {

	flumenode := &FlumeNode{BusinessName: businsess}
	watch := make(chan zk.Event)

	path := FLUME_PATH + "/" + businsess
	exist, _, err := self.session.Exists(path, watch)

	//存在该节点
	if !exist && nil == err {
		resp, err := self.session.Create(path, nil, zk.CreatePersistent, zk.AclOpen)
		if nil != err {
			log.Println("can't create flume path " + path + "," + err.Error())
			return nil
		} else {
			log.Println("create flume path succ ! " + resp)
		}
	} else if nil != err {
		log.Println("can't create path [" + path + "]!")
		return nil
	}

	childnodes, _, err := self.session.Children(path, watch)
	if nil != err {
		log.Println("get data from [" + path + "] fail! " + err.Error())
		return nil
	}

	//赋值新的Node
	flumenode.Flume = self.DecodeNode(childnodes)

	//监听数据变更
	go func() {
		select {
		case change := <-watch:
			switch change.Type {
			case zk.Created:
				existWatcher(path, Created)
			case zk.Deleted:
				existWatcher(path, Deleted)
			case zk.Changed:
				existWatcher(path, Changed)
			case zk.Child:
				//子节点发生变更，则获取全新的子节点
				childnodes, _, err := self.session.Children(path, nil)
				if nil != err {
					log.Println("recieve child's changes fail ! [" + path + "]  " + err.Error())
				} else {
					log.Printf("%s|child's changed %s", path, childnodes)
					childWatcher(path, self.DecodeNode(childnodes))
				}

			}
		}
	}()

	return flumenode
}

func (self *ZKManager) DecodeNode(paths []string) []HostPort {

	//也许这里可以通过每个path获取每个地址中的连接数配置
	// val, _, err := self.session.Get(path, nil)
	// if nil != err {
	// 	log.Println("获取[" + path + "] 失败!")
	// 	return nil
	// }

	// path的demo为  flume的 host:port_seqid
	flumenode := make(map[int]HostPort)
	idx := make([]int, 0)
	for _, path := range paths {
		split := strings.Split(path, "_")
		hostport := split[0] + ":" + split[1]
		seq, _ := strconv.ParseInt(split[2], 10, 8)
		flumenode[int(seq)] = NewHostPort(hostport)
		idx = append(idx, int(seq))
	}
	sort.Ints(idx)

	if len(idx) <= 1 {
		idx = idx[0:]
	} else {
		idx = idx[:len(idx)/2]
	}

	//由小到大排序
	flumehost := make([]HostPort, 0)
	//选取出一半数量最小的Host作为master
	for _, v := range idx {

		hp, _ := flumenode[v]
		flumehost = append(flumehost, hp)
	}

	log.Println("running node :%s|seq:%s", flumehost, idx)
	return flumehost
}

func (self *ZKManager) Close() {
	self.session.Close()
}
