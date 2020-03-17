package wrpc

import (
	"sync"
	"log"
    "github.com/samuel/go-zookeeper/zk"
)

type Register struct {
	ser *Server
	mutex sync.Mutex
}

func NewRegister(ser *Server) *Register {
	return &Register{ser: ser}
}

func (r *Register) registe(){
	r.mutex.Lock()  //加锁

	zkc := r.ser.GetZkClient()
	conf := r.ser.GetConf()

	var path string
	if conf.GetIp() != ""{
		path = zkc.GetAbsolutePath(conf.GetPath())
	}else{
		oldPath := zkc.GetAbsolutePath(conf.GetPath())
		//reset ip
		conf.SetLocalIp()
		path = zkc.GetAbsolutePath(conf.GetPath())
		if path != oldPath{
			//delete old path if local ip changed
			existsOldPath, stat, _ := zkc.GetConn().Exists(oldPath)
			if existsOldPath{
				derr := zkc.GetConn().Delete(path, stat.Version)
				if derr != nil{
					log.Println("ERROR- ", derr)
				}else{
					log.Println("delete old path: ", oldPath)
				}
			}
		}
	}
	
    log.Println("prelook register path: ", path)
    exists, _, _ := zkc.GetConn().Exists(path)
    if !exists {
	    _, err := zkc.GetConn().Create(path, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err != nil {
			log.Println("ERROR- ", err)
		}else{
			log.Println("registe server success.")
		}
    }
    
	r.mutex.Unlock() //解锁
}

func (r *Register) RegisteAndListen(){
	zkc := r.ser.GetZkClient()	
	if zkc == nil{
		return
	}
	
	conf := r.ser.GetConf()
	zkc.EnsurePath(zkc.GetAbsolutePath(conf.GetParentPath()), 0)
	r.registe()
	
	fun := func (event zk.Event) {
		switch event.State{
			case zk.StateConnected:
			    log.Println("Connection is connected.")
			    go r.registe()
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
	option(zkc.GetConn())
}

func (r *Register) Close(){
	if r.ser != nil{
		r.ser.Stop()
	}
}
