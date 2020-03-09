package wrpc

import (
	"time"
)

/**
@author shuai.chen
@created 2020年3月8日
队列实现
**/

type Element struct{
	Value interface{}
	Next *Element
}

type Queue struct{
	head *Element
	tail *Element
	length int
}

func NewQueue() *Queue{
	return new(Queue).Init()
}

/*
initializes queue
*/
func (q *Queue) Init() *Queue{
	q.length = 0
	q.head = nil
	q.tail = nil
	return q
}

func (q *Queue) Length() int{
	return q.length
}

func (q *Queue) Get() *Element {
	head := q.head
	if head == nil{
		return nil
	}
	
	q.head = head.Next
	if head.Next == nil{
		q.tail = nil
		q.length = 0
	}else{
		q.length --
	}
	return head
}

/*
Get element wait for timeout
@timeout millsecond
*/
func (q *Queue) GetWait(timeout int) *Element {
	if q.length <= 0 && timeout > 0{
		endtime := time.Now().UnixNano() / 1e6 + int64(timeout)
		for{
			if q.length <= 0{
				remaining := endtime - time.Now().UnixNano() / 1e6
				if remaining <= 0{ break }
			}else{ break }
		}
	}
	return q.Get()
}

/*
put element
*/
func (q *Queue) Put(e *Element) *Element {
	e.Next = nil
	if q.head == nil{
		q.head = e
		q.tail = e
		q.length = 1
	}else{
		tail := q.tail
		tail.Next = e
		q.tail = e
		q.length ++
	}
	return e
}

/*
put value
*/
func (q *Queue) PutValue(v interface{}) *Element {
	e := &Element{Value:v}
	return q.Put(e)
}
