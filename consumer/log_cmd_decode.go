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
	"strconv"
)

//解析出decodecommand
func decodeCommand(resp []byte) (string, string, *flume.ThriftFlumeEvent) {
	var cmd config.Command
	err := json.Unmarshal(resp, &cmd)
	if nil != err {
		log.Printf("command unmarshal fail ! %T | error:%s\n", resp, err.Error())
		return "", "", nil
	}

	if cmd.Params["momoid"] == nil {
		log.Printf("command momoid is nil %T\n", resp)
		return "", "", nil
	}

	momoid, ok := cmd.Params["momoid"].(string)
	if !ok {
		log.Printf("command format error. (momoid should be string type) %T\n", resp)
		return "", "", nil
	}

	if cmd.Params["businessName"] == nil {
		log.Printf("command format error. (businessName is nil) %T\n", resp)
		return "", "", nil
	}
	businessName, ok := cmd.Params["businessName"].(string)
	if !ok {
		log.Printf("command format error. (businessName should be string type) %T\n", resp)
		return "", "", nil
	}

	if cmd.Params["type"] == nil {
		log.Printf("command format error. (type is nil) %T\n", resp)
		return "", "", nil
	}

	action, ok := cmd.Params["type"].(string)
	if !ok {
		log.Printf("command format error. (type should be string type) %T\n", resp)
		return "", "", nil
	}

	bodyContent := cmd.Params["body"]
	bodyMap := bodyContent.(map[string]interface{})

	var logType string
	if cmd.Params["log_type"] != nil {
		logType = strconv.Itoa((int)(cmd.Params["log_type"].(float64)))
		bodyMap["log_type"] = logType
	}

	//将businessName 加入到body中
	bodyMap["business_type"] = businessName
	//if cmd.Params["log_type"] != nil {
	//    logType:= (int)(cmd.Params["log_type"].(float64))
	//    bodyMap["log_type"] = logType
	//}

	body, err := json.Marshal(bodyContent)
	if nil != err {
		log.Printf("marshal log body fail %s", err.Error())
		return businessName, logType, nil
	}

	//拼Body
	flumeBody := fmt.Sprintf("%s\t%s\t%s", momoid, action, string(body))
	obj := client.NewFlumeEvent()
	event := client.EventFillUp(obj, businessName+logType, action, []byte(flumeBody))
	return businessName, logType, event
}

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
