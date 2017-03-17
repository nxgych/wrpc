#coding:utf-8

'''
Created on 2017年3月6日
@author: shuai.chen
'''

import Queue
import threading

class ObjectPool(object):
    """object pool"""
    
    _lock = threading.RLock()
 
    def __init__(self, func, *args, **kwargs):
        """
        object pool class 
        @param func: method
        @param args: parmas of func  
        @param kwargs: 
            pool_max_size:  pool max size, default is 8    
            pool_wait_timeout:  pool block time, default is None means forever          
        """
        super(ObjectPool, self).__init__()
        self.func = func
        self.args = args
        self.max_size = kwargs.get("pool_max_size",8)
        self.wait_timeout = kwargs.get("pool_wait_timeout")
        
        #队列计数
        self.count = 0
        self.queue = Queue.Queue(self.max_size)
    
    def _get_obj(self):
        return self.func(*self.args)
    
    def __put_obj(self, obj):
        if self.queue.qsize() < self.max_size:
            self.queue.put(obj) 
        else:
            if hasattr(obj, "close"):
                try:
                    obj.close()  
                except:
                    pass      
    
    def borrow_obj(self):
        with self._lock:
            if self.empty() and self.count < self.queue.maxsize:
                self.queue.put(self._get_obj())
                self.count += 1
            return self.queue.get(True, self.wait_timeout) 
    
    def return_obj(self, obj):
        with self._lock:
            self.__put_obj(obj)    

class KeyedObjectPool(ObjectPool):    
    """
    keyed object pool 
    """
    
    def __init__(self, func, *args, **kwargs):    
        super(KeyedObjectPool, self).__init__(func, *args, **kwargs)
        self.queue_map = {} # {key:queue}
        self.queue = None
    
    def __check(self, key):
        if key not in self.queue_map:
            self.queue_map[key] = Queue.Queue(self.max_size)
     
    def __put_obj(self, obj, key):
        self.__check(key)
        if self.queue_map[key].qsize() < self.max_size:
            self.queue_map[key].put(obj) 
        else:
            if hasattr(obj, "close"):
                try:
                    obj.close()  
                except:
                    pass   
    
    def __contains__(self, key):
        return key in self.queue_map
                
    def __getitem__(self, key):  
        return self.borrow_obj(key)     
    
    def __setitem__(self, key, obj):   
        self.return_obj(obj, key)   
                    
    def borrow_obj(self, key):
        with self._lock:
            self.__check(key)
            if self.queue_map[key].empty() and self.count < self.queue_map[key].maxsize:
                self.args = (key,)
                self.queue_map[key].put(self._get_obj())
                self.count += 1
            return self.queue_map[key].get(True, self.wait_timeout) 
    
    def return_obj(self, obj, key):
        with self._lock:
            self.__put_obj(obj, key) 
