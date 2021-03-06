package wrpc

/**
@author shuai.chen
@created 2020年1月8日
**/

import (
	"net"
	"strconv"
	"reflect"
    "github.com/apache/thrift/lib/go/thrift"
)

type Server struct {
	zkc *ZkClient
	conf *ServerConfig
	serverObj *thrift.TSimpleServer
}

/*
create server
@zkc：zookeeper客户端
@conf: 服务配置
*/
func NewServer(zkc *ZkClient, conf *ServerConfig) *Server{	
	ser := &Server{zkc:zkc, conf:conf}
	ser.setServerObj()
	return ser
}

func (ser *Server) GetConf() *ServerConfig{
	return ser.conf
}

func (ser *Server) GetZkClient() *ZkClient{
	return ser.zkc
}

// set server object
func (ser *Server) setServerObj(){
	mprocessor := ser.getProcessor()
	ser.serverObj = ServerFactory(mprocessor, ser.conf.GetServerIp(), ser.conf.GetPort())
}

/*
 build muti processor
 */
func (ser *Server) getProcessor() *thrift.TMultiplexedProcessor{
	multiProcessor := thrift.NewTMultiplexedProcessor()
	for _, processor := range ser.conf.GetServiceProcessors(){
		//反射
		ref := reflect.ValueOf(processor).Elem()
		if ref.Kind() == reflect.Ptr{
		    ref = ref.Elem()
		}	
	    serviceName := ref.Type().Field(1).Type.Name()
		multiProcessor.RegisterProcessor(serviceName, processor)
	}
	return multiProcessor
}

// register
func (ser *Server) registerServer(){
	if ser.zkc != nil {
		register := NewRegister(ser)
		register.RegisteAndListen()
	}
}

/*
start server
*/
func (ser *Server) Start(){
    ser.registerServer() 
	// start
	if ser.serverObj != nil{
		ser.serverObj.Serve()
	}
}

/*
stop server
*/
func (ser *Server) Stop(){
	if ser.serverObj != nil{
		ser.serverObj.Stop()
	}
	if ser.zkc != nil {
		ser.zkc.Close()
	}
}

/*
 server factory
 */
func ServerFactory(processor *thrift.TMultiplexedProcessor, ip string, port int) *thrift.TSimpleServer{
	port_ := strconv.Itoa(port)
	serverTransport, err := thrift.NewTServerSocket(net.JoinHostPort(ip, port_))
	if err != nil {
	    panic(err)
	}
	
	protocolFactory := thrift.NewTCompactProtocolFactory()
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	
	// 创建server
	server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)
    return server
}
