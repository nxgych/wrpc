package wrpc

import (
	"bytes"
	"strconv"
    "github.com/apache/thrift/lib/go/thrift"
)

type ServerConfig struct {
	globalServiceName string
	serviceProcessors []thrift.TProcessor
	version string
	ip string
	port int
	weight int
	localIp string
}

/*
default server config
@globalServiceName：全局service名称
@serviceProcessors: service实现对象
*/
func NewDefaultServerConfig(globalServiceName string, serviceProcessors []thrift.TProcessor) *ServerConfig{
	return NewServerConfig(globalServiceName, serviceProcessors, "", "", 0, 0)
}

/*
server config
@globalServiceName：全局service名称
@serviceProcessors: service实现对象
@version：版本号，若传空字串，则默认为1.0.0
@ip: 服务IP，若传空字串，则默认为本地ip
@port: 服务端口号，若传0，则默认为 3068
@weight: 服务权重，若传0，则默认为1
*/
func NewServerConfig(globalServiceName string, serviceProcessors []thrift.TProcessor, version string, 
	                 ip string, port int, weight int) *ServerConfig{
	conf := ServerConfig{}
	
	conf.globalServiceName = globalServiceName
	conf.serviceProcessors = serviceProcessors
	conf.version = version
	conf.ip = ip
	conf.port = port
	conf.weight = weight
	conf.localIp = GetLocalIp()
	
	if version == ""{
		conf.version = VERSION_DEFAULT
	}
	if port <= 0{
		conf.port = PORT_DEFAULT
	}
	if weight <= 0{
		conf.weight = WEIGHT_DEFAULT
	}
	return &conf
}
	           
func (conf *ServerConfig) GetServiceProcessors() []thrift.TProcessor{
	return conf.serviceProcessors
}      

func (conf *ServerConfig) SetVersion(version string){
	conf.version = version
}  

func (conf *ServerConfig) GetIp() string{
	return conf.ip
}

func (conf *ServerConfig) GetServerIp() string {
	if conf.ip != ""{
		return conf.ip
	}
	return conf.localIp
}

func (conf *ServerConfig) SetIp(ip string){
	conf.ip = ip
}

func (conf *ServerConfig) SetLocalIp(){
	conf.localIp = GetLocalIp()
}

func (conf *ServerConfig) GetPort() int {
	return conf.port
}	 

func (conf *ServerConfig) SetPort(port int){
	conf.port = port
}

func (conf *ServerConfig) SetWeight(weight int){
	conf.weight = weight
}
          
/*
获取 zk 注册地址父级目录
*/
func (conf *ServerConfig) GetParentPath() string{
	var b bytes.Buffer
    b.WriteString(ZK_SEPARATOR_DEFAULT)
    if conf.globalServiceName != ""{
	    b.WriteString(conf.globalServiceName)
	    b.WriteString(ZK_SEPARATOR_DEFAULT)
    }
    b.WriteString(conf.version)
    return b.String()
}

func (conf *ServerConfig) GetPath() string{
	var b bytes.Buffer
    b.WriteString(conf.GetParentPath())
    b.WriteString(ZK_SEPARATOR_DEFAULT)
    b.WriteString(conf.GetNodeName())
    return b.String()
}

/*
获取节点名称
*/
func (conf *ServerConfig) GetNodeName() string{
	var b bytes.Buffer
    b.WriteString(conf.GetServerIp())
    b.WriteString(":")
    port := strconv.Itoa(conf.port)
    b.WriteString(port)
    b.WriteString(":")
    weight := strconv.Itoa(conf.weight)
    b.WriteString(weight)
    return b.String()
}
