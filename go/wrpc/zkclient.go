package wrpc

/**
@author shuai.chen
@created 2020年1月8日
**/

import(
	"strings"
	"time"
	"log"
	"fmt"
    "github.com/samuel/go-zookeeper/zk"
)


type ZkClient struct {
	address []string
	timeout int //second
	namespace string
    root string
	Conn *zk.Conn
}

/*
default zookeeper client
@address hosts, "ip:port,ip:port"
*/
func NewDefaultZkClient(address string) *ZkClient {
	hosts := strings.Split(address, ",")
	return NewZkClient(hosts, 0, "")
}

/*
zookeeper client
@address：host list, ["ip:port", "ip:port"]
@timeout：超时时间 若传0，则默认为10000ms，单位:millsecond
@namespace：命名空间，一般为空
*/
func NewZkClient(address []string, timeout int, namespace string) *ZkClient{
	to := 10000;if timeout > 0 { to = timeout }
	zkc := &ZkClient{address:address, timeout:to, namespace:namespace}
	zkc.Connect()
	return zkc
}

// zk connect
func (z *ZkClient) Connect(){
	option := zk.WithEventCallback(callback)
    conn, connChan, err := zk.Connect(z.address, time.Millisecond*time.Duration(z.timeout), option)
    if err != nil {
        panic(fmt.Sprintf("Zookeeper connect error- %v", err))
    }
    e := <-connChan
    if e.State == zk.StateConnected || e.State == zk.StateConnecting{
	    z.Conn = conn
	    z.root = ZK_SEPARATOR_DEFAULT + ZK_ROOT
	    if z.namespace != "" {
	    	    z.root = z.root + ZK_SEPARATOR_DEFAULT + z.namespace
	    }
	    z.EnsurePath(z.root, 0) 
    }
}

func callback(event zk.Event) {
    if event.State == zk.StateConnected{
    	    log.Println("Connection is connected.")
    }else if event.State == zk.StateDisconnected{
        log.Println("Connection is disconnected.")
    }else if event.State == zk.StateExpired{
        log.Println("Connection session is expired.")
    }else if event.State == zk.StateConnecting{
        log.Println("Connection is connecting.")
    }else{
        log.Println("Connection is unknown.")
    }
}

func (z *ZkClient) GetAbsolutePath(path string) string{
    return z.root + path
}

// ensure path
func (z *ZkClient) EnsurePath(path string, flag int){
	nodes := strings.Split(path, ZK_SEPARATOR_DEFAULT)
	aimPath := ""
	for _, node := range nodes{
		if node == ""{
			continue
		}
		aimPath = aimPath + ZK_SEPARATOR_DEFAULT + node
		exists, _, _ := z.Conn.Exists(aimPath)	
		if !exists {
			_, err := z.Conn.Create(aimPath, nil, int32(flag), zk.WorldACL(zk.PermAll))
			if err != nil {
				log.Println("ERROR- ", err)
			}
		}
	}
}

func (z *ZkClient) Close(){
	if z.Conn != nil{
		z.Conn.Close()
	}
}
