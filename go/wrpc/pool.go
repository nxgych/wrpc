package wrpc

/**
@author shuai.chen
@created 2020年1月8日
连接池实现
**/

import (
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
    List *Queue        //队列
    Count int          //引用计数
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
		      pb:&PoolBlock{List:NewQueue(), Count:0}}
	if maxSize <= 0 { p.maxSize = MAX_SIZE }
	if maxActiveSize <= 0 { p.maxActiveSize = MAX_ACTIVE_SIZE }
	if waitTimeout <= 0 { p.waitTimeout = WAIT_TIMEOUT }
	return &p
}

/**
pool interval method
**/

func (p *Pool) getObj() (interface{}, error){
	return p.function(p.args...)
}

func (p *Pool) putObj(obj *Element){
	p.mutex.Lock()  //加锁
	if p.Size() < p.maxSize {
		p.pb.List.Put(obj)
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
	return p.pb.List.Length()
}

// 清空连接池
func (p *Pool) Clear(){
	p.mutex.Lock()  //加锁
	
    var e *Element
    for{
    	    e = p.pb.List.Get()
    	    if e != nil{
    	    	    go closeObj(e)
    	    }else{
    	    	    break
    	    }
    }
    p.pb.Count = 0
    
	p.mutex.Unlock() //解锁
}

func (p *Pool) Borrow() (*Element, error){
	p.mutex.Lock()  //加锁
	defer p.mutex.Unlock() //解锁
	
    if p.Size() <= 0 && p.pb.Count < p.maxSize{
    	    genObj, err := p.getObj()
    	    if err != nil{
    	    	    return nil, err
    	    }
    	    
    	    p.pb.List.PutValue(genObj)
    	    p.pb.Count += 1
    }
    
    obj := p.pb.List.GetWait(p.waitTimeout)
	if obj != nil{
		return obj, nil
	}
	return nil, errors.New("Queue is empty.")
}

func (p *Pool) Return(obj *Element){
	if p.Size() < p.maxActiveSize{
	    p.putObj(obj)
	}else{
		p.Destroy(obj)
	}
}

func (p *Pool) Destroy(obj *Element){
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
	    p.pb[key] = &PoolBlock{List:NewQueue(), Count:0}
	}
}            

func (p *KeyedPool) getObj() (interface{}, error){
	return p.function(p.args...)
}

func (p *KeyedPool) putObj(obj *Element, key string){
	p.mutex.Lock() 
	p.check(key)
	if p.Size(key) < p.maxSize {
		p.pb[key].List.Put(obj)
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
	    return kp.List.Length()
	}
	return 0
}

// 清空连接池
func (p *KeyedPool) Clear(){
	p.mutex.Lock()  //加锁
	
	for k := range p.pb{
	    var e *Element
	    for{
	    	    e = p.pb[k].List.Get()
	    	    if e != nil{
	    	    	    go closeObj(e)
	    	    }else{
	    	    	    break
	    	    }
	    }
	    p.pb[k].Count = 0
	}
    
	p.mutex.Unlock() //解锁
}

func (p *KeyedPool) Borrow(key string) (*Element, error){
	p.mutex.Lock()  //加锁
	defer p.mutex.Unlock() //解锁
	
	p.check(key)
    if p.Size(key) <= 0 && p.pb[key].Count < p.maxSize{
    	    p.args = []string{key}
    	    
    	    genObj, err := p.getObj()
    	    if err != nil{
    	    	    return nil, err
    	    }
    	    
    	    p.pb[key].List.PutValue(genObj)
    	    p.pb[key].Count += 1
    }
    
    obj := p.pb[key].List.GetWait(p.waitTimeout)
	if obj != nil{
		return obj, nil
	}
	return nil, errors.New("Queue is empty.")
}

func (p *KeyedPool) Return(obj *Element, key string){
	if p.Size(key) < p.maxActiveSize{
	    p.putObj(obj, key) 
	}else{
		p.Destroy(obj, key)
	}
}

func (p *KeyedPool) Destroy(obj *Element, key string){
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
func closeObj(obj *Element){
	if obj != nil{
	    ref := reflect.ValueOf(obj.Value)
	    method := ref.MethodByName("Close")
	    if (method.IsValid()) {
	        method.Call([]reflect.Value{})
	    }
	}
}
