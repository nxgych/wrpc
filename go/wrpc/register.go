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
		switch event.State{
			case zk.StateConnected:
			    log.Println("Connection is connected.")
			    go r.Registe(c)
			case zk.StateDisconnected:
			    log.Println("Connection is disconnected.")
			case zk.StateExpired:
				log.Println("Connection session is expired.")
			case zk.StateConnecting:
				log.Println("Connection is connecting.")
			default:
				log.Println("Connection is unknown.")		    
		}
	}
	
	option := zk.WithEventCallback(fun)
	option(r.zkc.Conn)
}

func (r *Register) Registe(c *ServerConfig){
	r.mutex.Lock()  //加锁
	
	var path string
	if c.GetIp() != ""{
		path = r.zkc.GetAbsolutePath(c.GetPath())
	}else{
		oldPath := r.zkc.GetAbsolutePath(c.GetPath())
		//reset ip
		c.SetLocalIp()
		path = r.zkc.GetAbsolutePath(c.GetPath())
		if path != oldPath{
			//delete old path if local ip changed
			existsOldPath, stat, _ := r.zkc.Conn.Exists(oldPath)
			if existsOldPath{
				derr := r.zkc.Conn.Delete(path, stat.Version)
				if derr != nil{
					log.Println("ERROR- ", derr)
				}else{
					log.Println("delete old path: ", oldPath)
				}
			}
		}
	}
	
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

func (r *Register) Close(){
	if r.zkc != nil{
		r.zkc.Close()
	}
}
