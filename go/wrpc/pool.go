package wrpc

/**
@author shuai.chen
@created 2020年1月8日
连接池实现
**/

import (
	"time"
	"sync"
	"container/list"
	"reflect"
	"errors"
)

type CreateFuncType func(...string) (interface{}, error)

// pool block
type PoolBlock struct {
    List *list.List    //模拟队列
    Count int          //引用计数
}

/**
@timeout millsecond
*/
func (pb *PoolBlock) Front(timeout int) (*list.Element, error){
	if timeout > 0 && pb.List.Len() <= 0{
		endtime := time.Now().UnixNano() / 1e6 + int64(timeout)
		for{
			if pb.List.Len() <= 0{
				remaining := endtime - time.Now().UnixNano() / 1e6
				if remaining <= 0{ break }
			}else{ break }
		}
	}
	obj := pb.List.Front()
	if obj != nil{
		return obj, nil
	}
	return nil, errors.New("List is empty.")
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

/**
pool external method
**/

func NewPool(function CreateFuncType, maxSize int, maxActiveSize int, waitTimeout int) *Pool{
	p := Pool{BasePool:BasePool{function:function, maxSize:maxSize, maxActiveSize:maxActiveSize,
			                    waitTimeout:waitTimeout}, 
		      pb:&PoolBlock{List:list.New(), Count:0}}
	if maxSize <= 0 { p.maxSize = 8 }
	if maxActiveSize <= 0 { p.maxActiveSize = 4 }
	if waitTimeout <= 0 { p.waitTimeout = 10000 }
	return &p
}

// 获取连接池大小
func (p *Pool) Size() int{
	return p.pb.List.Len()
}

// 清空连接池
func (p *Pool) Clear(){
	p.mutex.Lock()  //加锁
	
    var n *list.Element
    for e := p.pb.List.Front(); e != nil; e = n {
    	    closeObj(e)
        n = e.Next()
        p.pb.List.Remove(e)
    }
    p.pb.Count = 0
    
	p.mutex.Unlock() //解锁
}

func (p *Pool) Borrow() (*list.Element, error){
	p.mutex.Lock()  //加锁
	
    if p.Size() <= 0 && p.pb.Count < p.maxSize{
    	    genObj, err := p.getObj()
    	    if err != nil{
    	    	    p.mutex.Unlock() //解锁
    	    	    return nil, err
    	    }
    	    
    	    p.pb.List.PushBack(genObj)
    	    p.pb.Count += 1
    }
    obj, ferr := p.pb.Front(p.waitTimeout)
    
	p.mutex.Unlock() //解锁
	return obj, ferr
}

func (p *Pool) Return(obj *list.Element){
	if p.Size() <= p.maxActiveSize{
	    p.putObj(obj)
	}else{
		p.Destroy(obj)
	}
}

func (p *Pool) Destroy(obj *list.Element){
	if obj != nil{
		p.mutex.Lock()  //加锁
	    closeObj(obj)
	    if p.pb.Count > 0{
	    	    p.pb.Count -= 1
	    }
		p.mutex.Unlock() //解锁
	}
}

/**
pool interval method
**/

func (p *Pool) getObj() (interface{}, error){
	return p.function(p.args...)
}

func (p *Pool) putObj(obj *list.Element){
	p.mutex.Lock()  //加锁
	if p.Size() < p.maxSize {
		p.pb.List.PushBack(obj)
	}else{
		closeObj(obj)
	}
	p.mutex.Unlock() //解锁	
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
	if maxSize <= 0 { kp.maxSize = 8 }
	if maxActiveSize <= 0 { kp.maxActiveSize = 4 }
	if waitTimeout <= 0 { kp.waitTimeout = 10000 }
	return &kp
	
}

/**
keyedPool external method
**/

// 获取连接池大小
func (p *KeyedPool) Size(key string) int{
	kp, ok := p.pb[key]
	if ok{
	    return kp.List.Len()
	}
	return 0
}

// 清空连接池
func (p *KeyedPool) Clear(){
	p.mutex.Lock()  //加锁
	
	for k := range p.pb{
	    var n *list.Element
	    for e := p.pb[k].List.Front(); e != nil; e = n {
	    	    closeObj(e)
	        n = e.Next()
	        p.pb[k].List.Remove(e)
	    }
	    p.pb[k].Count = 0
	}
    
	p.mutex.Unlock() //解锁
}

func (p *KeyedPool) Borrow(key string) (*list.Element, error){
	p.mutex.Lock()  //加锁
	
	p.check(key)
    if p.Size(key) <= 0 && p.pb[key].Count < p.maxSize{
    	    p.args = []string{key}
    	    
    	    genObj, err := p.getObj()
    	    if err != nil{
    	    	    p.mutex.Unlock() //解锁
    	    	    return nil, err
    	    }
    	    
    	    p.pb[key].List.PushBack(genObj)
    	    p.pb[key].Count += 1
    }
    obj, ferr := p.pb[key].Front(p.waitTimeout)
    
	p.mutex.Unlock() //解锁
	return obj, ferr
}

func (p *KeyedPool) Return(obj *list.Element, key string){
	if p.Size(key) <= p.maxActiveSize{
	    p.putObj(obj, key)
	}else{
		p.Destroy(obj, key)
	}
}

func (p *KeyedPool) Destroy(obj *list.Element, key string){
	if obj != nil{
		p.mutex.Lock()  //加锁
	    closeObj(obj)
	    if p.pb[key].Count > 0{
	    	    p.pb[key].Count -= 1
	    }
		p.mutex.Unlock() //解锁
	}
}

/**
keyedPool interval method
**/

func (p *KeyedPool) check(key string){
	_, ok := p.pb[key]
	if !ok{
	    p.pb[key] = &PoolBlock{List:list.New(), Count:0}
	}
}            

func (p *KeyedPool) getObj() (interface{}, error){
	return p.function(p.args...)
}

func (p *KeyedPool) putObj(obj *list.Element, key string){
	p.mutex.Lock()  //加锁
	p.check(key)
	if p.Size(key) < p.maxSize {
		p.pb[key].List.PushBack(obj)
	}else{
		closeObj(obj)
	}
	p.mutex.Unlock() //解锁
}

// 关闭对象
func closeObj(obj *list.Element){
	if obj != nil{
	    ref := reflect.ValueOf(obj.Value)
	    method := ref.MethodByName("Close")
	    if (method.IsValid()) {
	        method.Call([]reflect.Value{})
	    }
	}
}
