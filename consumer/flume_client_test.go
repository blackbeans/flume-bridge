package consumer

import (
	"testing"

	"encoding/json"
)

func TestFlumeClient(t *testing.T) {
	client := newFlumeClient("localhost", 61111)
	client.connect()
	innerTest(client, t)
	defer client.destory()
}

type LogDemo struct {
	ViewSelf       int32  `json:"view_self"`
	RemoteId       string `json:"remoteid"`
	Timestamp      int64  `json:"timestamp"`
	FlumeTimestamp string `json:"flume_timestamp"`
	BusinessType   string `json:"business_type"`
}

func innerTest(client *flumeClient, t testing.TB) {

	//header: {businessName=feed, type=list}.
	//body: 100311	list	{"view_self":0,"remoteid":"5445285","timestamp":1403512030,"flume_timestamp":"2014-06-23 16:27:10","business_type":"feed"}
	body := "{\"view_self\":0,\"remoteid\":\"5445285\",\"timestamp\":1403512030,\"flume_timestamp\":\"2014-06-23 16:27:10\",\"business_type\":\"feed\"}"

	var demo LogDemo
	err := json.Unmarshal([]byte(body), &demo)
	if nil != err {
		t.Fail()
		return
	}

	data, err := json.Marshal(demo)
	if nil != err {
		t.Fail()
		return
	}

	header := make(map[string]string, 2)
	header["businessName"] = "feed"
	header["type"] = "list"

	for i := 0; i < 1; i++ {

		err := client.append(header, data)
		if nil != err {
			t.Log(err.Error())
			t.Fail()

		} else {
			t.Logf("%d, send succ ", i)
		}
	}
}

func BenchmarkFlumeClient(t *testing.B) {
	client := newFlumeClient("localhost", 61111)
	client.connect()
	for i := 0; i < t.N; i++ {
		innerTest(client, t)
	}
	defer client.destory()

}
