package wrpc

/**
@author shuai.chen
@created 2020年1月8日
**/

import (
	"bytes"
	"sync"
	"strings"
	"log"
    "github.com/samuel/go-zookeeper/zk"
)

type Provider interface {
	GetServices() []Service
	SetClientPool(pool *ClientPool)
	SetNodes()
	Select() (*Node, error)
}

type BaseProvider struct {
	allNodes map[string]*Node
	liveNodes []*Node
	loadBalance LoadBalance
	services []Service
}

/**
AutoProvider
**/
type AutoProvider struct {
	BaseProvider
	
	zkc *ZkClient
    clientPool *ClientPool

	globalServiceName string
	version string
	
    mutex sync.Mutex
}

/*
default auto provider
@zkc：zookeeper客户端
@services：service 列表
@globalServiceName：全局service名称，和server保持一致
*/
func NewDefaultAutoProvider(zkc *ZkClient, services []Service, globalServiceName string) *AutoProvider {
	return NewAutoProvider(zkc, services, globalServiceName, RANDOM, "")
}

/*
auto provider
@zkc：zookeeper客户端
@services：service 列表
@globalServiceName：全局service名称，和server保持一致
@loadBalanceType：负载均衡类型
@version：版本号，若传空字串，则默认为1.0.0
*/
func NewAutoProvider(zkc *ZkClient, services []Service, globalServiceName string,
                     loadBalanceType int, version string) *AutoProvider {
    provider := AutoProvider{} 
    provider.zkc = zkc
    provider.services = services        
    provider.loadBalance = GetLoadBalance(loadBalanceType) 
    provider.globalServiceName = globalServiceName
    provider.version = version 	
    	if version == ""{
		provider.version = VERSION_DEFAULT
	} 
    	provider.allNodes = map[string]*Node{}                	
	return &provider
}

/*
获取 zk 注册地址父级目录
*/
func (ap *AutoProvider) GetParentPath() string{
	var b bytes.Buffer
    b.WriteString(ZK_SEPARATOR_DEFAULT)
    if ap.globalServiceName != ""{
	    b.WriteString(ap.globalServiceName)
	    b.WriteString(ZK_SEPARATOR_DEFAULT)
    }
    b.WriteString(ap.version)
    return b.String()
}

func (ap *AutoProvider) GetServices() []Service {
	return ap.services
}

func (ap *AutoProvider) SetNodes(){    
    path := ap.zkc.GetAbsolutePath(ap.GetParentPath())
    
    nodes, _, child_ch, err := ap.zkc.Conn.ChildrenW(path)
    if err != nil {
    	    log.Println("get zk path: ", path)
        log.Println("ERROR- ", err)
    }else{
	    ap.mutex.Lock()  //加锁
	    
	    ap.liveNodes = (ap.liveNodes)[0:0]
	    if len(nodes) <= 0{
	    	    log.Println("server not found!")
	    }
	    
	    for _, nodeStr := range nodes{
	        serverNode := ServerNode(nodeStr)
	        ap.allNodes[nodeStr] = serverNode
	        ap.liveNodes = append(ap.liveNodes, serverNode)
	    }
	    ap.loadBalance.SetNodes(ap.liveNodes) 
	    ap.clientPool.ClearPool() 
	    
	    	ap.mutex.Unlock() //解锁   
    }
    go ap.watch(child_ch)
}

// watch the child events
func (ap *AutoProvider) watch(child_ch <-chan zk.Event){
    for{
	    select {
	    	case e := <-child_ch:
	    	    if e.Type == zk.EventNodeChildrenChanged {
				ap.SetNodes() 			    	
				log.Println("Child node changed.") 
	    	    }else if e.Type == zk.EventNodeCreated{
				ap.SetNodes() 			    	
				log.Println("Child node created.") 
	    	    }else if e.Type == zk.EventNodeDeleted{
				ap.SetNodes() 			    	
				log.Println("Child node deleted.") 
	    	    }
	    }
    }	
}

func (ap *AutoProvider) SetClientPool(pool *ClientPool){
	ap.clientPool = pool
}

func (ap *AutoProvider) Select() (*Node, error){
	node, err := ap.loadBalance.GetNode()
	return node, err
}

/**
FixedProvider
**/
type FixedProvider struct {
	BaseProvider
	serverAddress string
}

/*
fixed provider
@services：service 列表
@serverAddress: server address  "ip:port:weight," or "ip:port,"
@loadBalanceType：负载均衡类型
*/
func NewFixedProvider(services []Service, serverAddress string, loadBalanceType int) *FixedProvider {
    loadBalance := GetLoadBalance(loadBalanceType)    
    bp := BaseProvider{services:services, loadBalance:loadBalance, allNodes:map[string]*Node{}}             	
	return &FixedProvider{BaseProvider:bp, serverAddress:serverAddress}
}

func (fp *FixedProvider) GetServices() []Service {
	return fp.services
}

func (fp *FixedProvider) SetNodes(){
	nodes := strings.Split(fp.serverAddress, ",")
    for _, nodeStr := range nodes{
        serverNode := ServerNode(nodeStr)
        fp.liveNodes = append(fp.liveNodes, serverNode)
    }
    fp.loadBalance.SetNodes(fp.liveNodes)    
}

func (fp *FixedProvider) SetClientPool(pool *ClientPool){

}

func (fp *FixedProvider) Select() (*Node, error){
	node, err := fp.loadBalance.GetNode()
	return node, err
}

