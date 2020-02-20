package wrpc

import (
	"errors"
	"sync"
	"math/rand"
)

const (
	RANDOM = 1
	ROUNDROBIN = 2
)

type LoadBalance interface{
	SetNodes(nodes []*Node)
	GetNode() (*Node, error)
}

type BaseLoadBalance struct {
	nodes []*Node
}

func GetLoadBalance(loadBalaceType int) LoadBalance{
    switch loadBalaceType {
	    case RANDOM:
	        return &Random{}
	    case ROUNDROBIN:
	        return &RoundRobin{}
	    default:
	        return &Random{}
    }
}

/*
随机方式
*/
type Random struct {
	BaseLoadBalance
}

/**
set nodes
*/
func (lb *Random) SetNodes(nodes []*Node){
	lb.nodes = transfer(nodes)
}

/**
get nodes
*/
func (lb *Random) GetNode() (*Node, error){
	length := len(lb.nodes) 
	if length <= 0{
		return nil, errors.New("Server not found!")
	}
	return lb.nodes[rand.Intn(length)], nil
}


/*
轮训方式
*/
type RoundRobin struct {
	BaseLoadBalance
	pos int
	mutex sync.Mutex 
}

/**
set nodes
*/
func (lb *RoundRobin) SetNodes(nodes []*Node){
	lb.nodes = transfer(nodes)
	lb.pos = 0
}

/**
get nodes
*/
func (lb *RoundRobin) GetNode() (*Node, error){
	length := len(lb.nodes) 
	if length <= 0{
		return nil, errors.New("Server not found!")
	}
	
	lb.mutex.Lock()  //加锁
	if lb.pos >= length{
		lb.pos = 0
	}
	node := lb.nodes[lb.pos]
	lb.pos += 1
	lb.mutex.Unlock() //解锁
	
	return node, nil
}

/**
节点按权重转换
*/
func transfer(nodes []*Node) []*Node{
	var nodeList []interface{}
	for _, node := range(nodes){
		for i:=0; i<node.GetWeight(); i++ {
			nodeList = append(nodeList, node)
		}
	}
	Shuffle(nodeList)
	
    var nList []*Node
    for _, param := range nodeList {
        nList = append(nList, param.(*Node))
    }
	return nList
}
