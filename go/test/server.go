package main

import (
	"log"
	"context"
    "github.com/apache/thrift/lib/go/thrift"
    "github.com/nxgych/wrpc/go/wrpc"
    tm "test/message"
)

type Message struct {
}

func(m *Message) SendSMS(c context.Context, mobile string, context string) (bool, error){
	log.Println("mobile: ", mobile)
	return true, nil
}

func main() {
	address := "127.0.0.1:2181"
	zkc := wrpc.NewDefaultZkClient(address)
	
	processors := []thrift.TProcessor{tm.NewMessageServiceProcessor(new(Message))}
	conf := wrpc.NewDefaultServerConfig("com.wrpc.test", processors)
	
	server := wrpc.NewServer(zkc, conf)
	server.Start()
	defer server.Stop()
}
