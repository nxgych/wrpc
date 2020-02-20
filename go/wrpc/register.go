package wrpc

import (
	"sync"
	"log"
    "github.com/samuel/go-zookeeper/zk"
)

type Register struct {
	zkc *ZkClient
	mutex sync.Mutex
}

func NewRegister(zkc *ZkClient) *Register {
	return &Register{zkc: zkc}
}

func (r *Register) RegisteAndListen(c *ServerConfig){
	if r.zkc == nil{
		return
	}
	r.zkc.EnsurePath(r.zkc.GetAbsolutePath(c.GetParentPath()), 0)
	r.Registe(c)
	
	fun := func (event zk.Event) {
	    if event.State == zk.StateConnected{
	    	    log.Println("Connection is connected.")
	    	    go r.Registe(c)
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
	
	option := zk.WithEventCallback(fun)
	option(r.zkc.Conn)
}

func (r *Register) Registe(c *ServerConfig){
	r.mutex.Lock()  //加锁
	
    path := r.zkc.GetAbsolutePath(c.GetPath())
    log.Println("prelook register path: ", path)
    
    exists, _, _ := r.zkc.Conn.Exists(path)
    if !exists {
	    _, err := r.zkc.Conn.Create(path, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err != nil {
			log.Println("ERROR- ", err)
		}else{
			log.Println("registe server success.")
		}
    }
    
	r.mutex.Unlock() //解锁
}
