package wrpc

/**
@author shuai.chen
@created 2020年1月8日
**/

import (
	"net"
	"time"
	"log"
	"reflect"
	"errors"
	"fmt"
	"strconv"
    "github.com/apache/thrift/lib/go/thrift"
)

type ClientFuncType func(param *thrift.TStandardClient) interface{}

type Service interface{
	GetService() interface{}
	GetServiceClient(cli *thrift.TStandardClient) interface{}
}

//client
type Client struct {
	proxyMap map[string]*ClientProxy
}

/*
create default client
@provider 服务提供对象
*/
func NewDefaultClient(provider Provider) *Client{
	return NewClient(provider, 0, 0, 0, 0, 0)
}

/*
create client
@provider 服务提供对象
@retry 访问重试次数，若传0，则默认为3
@retryInterval  重试间隔，若传0，则默认200， 单位:Millisecond
@poolMaxSize 客户端连接池最大连接数，若传0，则默认为8
@poolMaxActiveSize 客户端连接池最大活跃连接数，若传0，则默认为4
@poolWaitTimeout  客户端连接池等待时长，传0，则默认10000ms，则单位:Millisecond
*/
func NewClient(provider Provider, retry int, retryInterval int, 
	           poolMaxSize int, poolMaxActiveSize int, poolWaitTimeout int) *Client{
	services := provider.GetServices()	
	
	clientMap := map[string]ClientFuncType{}
	for _, service := range services{
		serviceName := reflect.TypeOf(service.GetService()).Elem().Name()
		clientMap[serviceName] = ClientFuncType(service.GetServiceClient)
	}
	clientFactory := &ClientFactory{provider:provider, clientMap:clientMap}
	clientPool := NewClientPool(clientFactory, poolMaxSize, poolMaxActiveSize, poolWaitTimeout)
	
	proxyMap := map[string]*ClientProxy{}
	for sn := range clientMap{
		proxyMap[sn] = NewClientProxy(sn, clientPool, retry, retryInterval)
	}
	
	provider.SetClientPool(clientPool)
	provider.SetNodes()
	time.Sleep(1*time.Second)
	
	return &Client{proxyMap:proxyMap}
}

func (c *Client) GetClient(key string) *ClientProxy{
	return c.proxyMap[key]
}

func (c *Client) Call(key string, method string, args ...interface{}) (interface{}, error){
	result, err := c.proxyMap[key].Call(method, args)
	return result, err
}

/*
client factory
*/
type ClientFactory struct{
	provider Provider
	clientMap map[string]ClientFuncType
}

func (c *ClientFactory) Create(key ...string) (interface{}, error){
	serverNode, errSelect := c.provider.Select()
	if errSelect != nil{
		return nil, errSelect
	}
	
	port_ := strconv.Itoa(serverNode.GetPort())
    socket, errSocket := thrift.NewTSocket(net.JoinHostPort(serverNode.GetAddress(), port_))
    if errSocket != nil {
        return nil, errSocket
    }
    
    transport := thrift.NewTFramedTransport(socket)
    protocol := thrift.NewTCompactProtocol(transport)
    // 打开Transport，与服务器进行连接
    if err := transport.Open(); err != nil {
        return nil, err
    }
    
    mProtocol := thrift.NewTMultiplexedProtocol(protocol, key[0])
    cli := thrift.NewTStandardClient(mProtocol, mProtocol)
    client := c.clientMap[key[0]](cli)
    return client, nil
}

// client pool
type ClientPool struct{
	pool *KeyedPool
}

/*
create client pool
*/
func NewClientPool(clientFactory *ClientFactory, poolMaxSize int, poolMaxActiveSize int, 
	               poolWaitTimeout int) *ClientPool{
	pool := NewKeyedPool(clientFactory.Create, poolMaxSize, poolMaxActiveSize, poolWaitTimeout)
	return &ClientPool{pool}
}

func (cp *ClientPool) GetPool() *KeyedPool{
	return cp.pool
}

func (cp *ClientPool) ClearPool() {
	cp.pool.Clear()
	log.Println("Client pool cleared.")  
}


// client proxy
type ClientProxy struct {
	serviceName string
	pool *ClientPool
	retry int
	retryInterval int //Millisecond
}

func NewClientProxy(serviceName string, pool *ClientPool, retry int, retryInterval int) *ClientProxy{
	clientProxy := ClientProxy{serviceName:serviceName, pool:pool, 
		                       retry:retry, retryInterval:retryInterval}
	if retry <= 0{ clientProxy.retry = 3 }
	if retryInterval <= 0{ clientProxy.retryInterval = 200 }
	return &clientProxy
}

/*
方法调用
@method: 方法名称
@args: 方法参数
*/
func (cp *ClientProxy) Call(method string, args... interface{}) (interface{}, error){
	key := cp.serviceName
	var res interface{}
	var err error
	
	for i := 0; i < cp.retry; i++{
		obj, errBorrow := cp.pool.GetPool().Borrow(key)
		
		if obj != nil{
		    ref := reflect.ValueOf(obj.Value)
		    f := ref.MethodByName(method)
		    if (f.IsValid()) {
		       	vals := make([]reflect.Value, 0, len(args))
				for i := range args {
					vals = append(vals, reflect.ValueOf(args[i]))
				}
		        result := f.Call(vals)
		        res = result[0]
		        
		        merr := result[1].Interface()
		        if merr != nil{
		            err = merr.(error)
		        }else{
		        	    err = nil
		        }
		    }else{
		    	    res, err = nil, errors.New(fmt.Sprintf("Unknown method '%s'.", method))
		    }
		    
		    //return object
		    cp.pool.GetPool().Return(obj, key)
		    
		    return res, err
		}else{
			// destroy object
			cp.pool.GetPool().Destroy(obj, key)
		}
		
		res, err = nil, errBorrow
		time.Sleep(time.Duration(cp.retryInterval) * time.Millisecond)
	}
	return res, err
}
