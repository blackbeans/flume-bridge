package consumer

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"flume-bridge/config"
	"flume-bridge/consumer/client"
	"flume-bridge/rpc/flume"
	"fmt"
	"io/ioutil"
	"log"
)

func decompress(data []byte) []byte {
	//log.Printf("decompress|INPUT|%T\n", data)
	r := flate.NewReader(bytes.NewBuffer(data))
	defer r.Close()
	ret, err := ioutil.ReadAll(r)
	if err != nil {
		log.Printf("command decompress fail ! | error:%s\n", err.Error())
		return nil
	}
	//log.Printf("decompress|OUTPUT|%s\n", string(ret))
	return ret
}

//解析出decodecommand
func decodeCommand(resp []byte) (string, *flume.ThriftFlumeEvent) {
	var cmd config.Command
	err := json.Unmarshal(resp, &cmd)
	if nil != err {
		log.Printf("command unmarshal fail ! %T | error:%s\n", resp, err.Error())
		return "", nil
	}
	//
	momoid := cmd.Params["momoid"].(string)

	businessName := cmd.Params["businessName"].(string)

	action := cmd.Params["type"].(string)

	bodyContent := cmd.Params["body"]

	//将businessName 加入到body中
	bodyMap := bodyContent.(map[string]interface{})
	bodyMap["business_type"] = businessName

	body, err := json.Marshal(bodyContent)
	if nil != err {
		log.Printf("marshal log body fail %s", err.Error())
		return businessName, nil
	}

	//拼Body
	flumeBody := fmt.Sprintf("%s\t%s\t%s", momoid, action, string(body))
	obj := client.NewFlumeEvent()
	event := client.EventFillUp(obj, businessName, action, []byte(flumeBody))
	return businessName, event
}
