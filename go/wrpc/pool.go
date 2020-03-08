package wrpc

/**
@author shuai.chen
@created 2020年1月8日
连接池实现
**/

import (
	"time"
	"sync"
	"reflect"
	"errors"
)

const MAX_SIZE int = 8
const MAX_ACTIVE_SIZE int = 4
const WAIT_TIMEOUT int = 10000 //ms

type CreateFuncType func(...string) (interface{}, error)

// pool block
type PoolBlock struct {
    List []interface{}    //模拟队列
    Count int          //引用计数
}

func (pb *PoolBlock) Size() int{
	return len(pb.List)
}

/**
@timeout millsecond
*/
func (pb *PoolBlock) Get(timeout int) (interface{}, error){
	if pb.Size() <= 0 && timeout > 0{
		endtime := time.Now().UnixNano() / 1e6 + int64(timeout)
		for{
			if pb.Size() <= 0{
				remaining := endtime - time.Now().UnixNano() / 1e6
				if remaining <= 0{ break }
			}else{ break }
		}
	}
	if pb.Size() > 0{
		obj := pb.List[0]
		if obj != nil{
			pb.List = pb.List[1:]
			return obj, nil
		}
	}
	return nil, errors.New("List is empty.")
}

func (pb *PoolBlock) Put(obj interface{}){
	pb.List = append(pb.List, obj)
}

//base pool struct
type BasePool struct {
	maxSize int
	maxActiveSize int
	waitTimeout int //Millisecond
	function CreateFuncType
	args []string
	mutex sync.Mutex 
}

//pool struct
type Pool struct {
	BasePool
    pb *PoolBlock
}

func NewPool(function CreateFuncType, maxSize int, maxActiveSize int, waitTimeout int) *Pool{
	p := Pool{BasePool:BasePool{function:function, maxSize:maxSize, maxActiveSize:maxActiveSize,
			                    waitTimeout:waitTimeout}, 
		      pb:&PoolBlock{Count:0}}
	if maxSize <= 0 { p.maxSize = MAX_SIZE }
	if maxActiveSize <= 0 { p.maxActiveSize = MAX_ACTIVE_SIZE }
	if waitTimeout <= 0 { p.waitTimeout = WAIT_TIMEOUT }
	p.pb.List = make([]interface{}, 0, p.maxSize)
	return &p
}

/**
pool interval method
**/

func (p *Pool) getObj() (interface{}, error){
	return p.function(p.args...)
}

func (p *Pool) putObj(obj interface{}){
	p.mutex.Lock()  //加锁
	if p.Size() < p.maxSize {
		p.pb.Put(obj)
	}else{
		go closeObj(obj)
	}
	p.mutex.Unlock() //解锁
}

/**
pool external method
**/

// 获取连接池大小
func (p *Pool) Size() int{
	return len(p.pb.List)
}

// 清空连接池
func (p *Pool) Clear(){
	p.mutex.Lock()  //加锁
	
	for _, e := range p.pb.List{
		go closeObj(e)
	}
    p.pb.List = p.pb.List[0:0]
    p.pb.Count = 0
    
	p.mutex.Unlock() //解锁
}

func (p *Pool) Borrow() (interface{}, error){
	defer p.mutex.Unlock() //解锁
	p.mutex.Lock()  //加锁
	
    if p.Size() <= 0 && p.pb.Count < p.maxSize{
    	    genObj, err := p.getObj()
    	    if err != nil{
    	    	    return nil, err
    	    }
    	    
    	    p.pb.Put(genObj)
    	    p.pb.Count += 1
    }
    obj, ferr := p.pb.Get(p.waitTimeout)
	return obj, ferr
}

func (p *Pool) Return(obj interface{}){
	if p.Size() < p.maxActiveSize{
	    p.putObj(obj)
	}else{
		p.Destroy(obj)
	}
}

func (p *Pool) Destroy(obj interface{}){
	if obj != nil{
		p.mutex.Lock()  //加锁
	    go closeObj(obj)
	    if p.pb.Count > 0{
	    	    p.pb.Count -= 1
	    }
		p.mutex.Unlock() //解锁
	}
}


//keyedPool struct
type KeyedPool struct {
	BasePool
	pb map[string]*PoolBlock
}

func NewKeyedPool(function CreateFuncType, maxSize int, maxActiveSize int, waitTimeout int) *KeyedPool{
	kp := KeyedPool{BasePool:BasePool{function:function, maxSize: maxSize, maxActiveSize: maxActiveSize,
			                          waitTimeout: waitTimeout}, 
		            pb:map[string]*PoolBlock{}}
	if maxSize <= 0 { kp.maxSize = MAX_SIZE } //max size per key
	if maxActiveSize <= 0 { kp.maxActiveSize = MAX_ACTIVE_SIZE } //max active size per key
	if waitTimeout <= 0 { kp.waitTimeout = WAIT_TIMEOUT }
	return &kp
	
}

/**
keyedPool interval method
**/

func (p *KeyedPool) check(key string){
	_, ok := p.pb[key]
	if !ok{
	    p.pb[key] = &PoolBlock{List:make([]interface{}, 0, p.maxSize), Count:0}
	}
}            

func (p *KeyedPool) getObj() (interface{}, error){
	return p.function(p.args...)
}

func (p *KeyedPool) putObj(obj interface{}, key string){
	p.mutex.Lock() 
	p.check(key)
	if p.Size(key) < p.maxSize {
		p.pb[key].Put(obj)
	}else{
		go closeObj(obj)
	}
	p.mutex.Unlock() //解锁
}

/**
keyedPool external method
**/

// 获取连接池大小
func (p *KeyedPool) Size(key string) int{
	kp, ok := p.pb[key]
	if ok{
	    return len(kp.List)
	}
	return 0
}

// 清空连接池
func (p *KeyedPool) Clear(){
	p.mutex.Lock()  //加锁
	
	for k := range p.pb{
		for _, e := range p.pb[k].List{
			go closeObj(e)
		}
        p.pb[k].List = p.pb[k].List[0:0]
	    p.pb[k].Count = 0
	}
    
	p.mutex.Unlock() //解锁
}

func (p *KeyedPool) Borrow(key string) (interface{}, error){
	defer p.mutex.Unlock() //解锁
	p.mutex.Lock()  //加锁
	
	p.check(key)
    if p.Size(key) <= 0 && p.pb[key].Count < p.maxSize{
    	    p.args = []string{key}
    	    
    	    genObj, err := p.getObj()
    	    if err != nil{
    	    	    return nil, err
    	    }
    	    
    	    p.pb[key].Put(genObj)
    	    p.pb[key].Count += 1
    }
    obj, ferr := p.pb[key].Get(p.waitTimeout)
	return obj, ferr
}

func (p *KeyedPool) Return(obj interface{}, key string){
	if p.Size(key) < p.maxActiveSize{
	    p.putObj(obj, key) 
	}else{
		p.Destroy(obj, key)
	}
}

func (p *KeyedPool) Destroy(obj interface{}, key string){
	if obj != nil{
		p.mutex.Lock()  //加锁
	    go closeObj(obj)
	    if p.pb[key].Count > 0{
	    	    p.pb[key].Count -= 1
	    }
		p.mutex.Unlock() //解锁
	}
}

// 关闭对象
func closeObj(obj interface{}){
	if obj != nil{
	    ref := reflect.ValueOf(obj)
	    method := ref.MethodByName("Close")
	    if (method.IsValid()) {
	        method.Call([]reflect.Value{})
	    }
	}
}
