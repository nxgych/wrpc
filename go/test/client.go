package main

import (
	"fmt"
	"context"
    "github.com/apache/thrift/lib/go/thrift"
    "wrpc"
    tm "test/message"  
)

type Message struct {
}

func (m *Message) GetService() interface{} {
	return (*tm.MessageService)(nil)
}

func (m *Message) GetServiceClient(cli *thrift.TStandardClient) interface{} {
	return tm.NewMessageServiceClient(cli)
}

func main() {
	address := "127.0.0.1:2181"
	zkc := wrpc.NewDefaultZkClient(address)
	
	services := []wrpc.Service{new(Message)}	
    provider := wrpc.NewDefaultAutoProvider(zkc, services, "com.wrpc.test")
    
    client := wrpc.NewDefaultClient(provider)
    service := client.GetClient("MessageService")
    
    res, err := service.Call("SendSMS", context.Background(), "110", "hello, world")
    if err != nil {
        fmt.Println(err)
    }
    fmt.Println(res)
}

