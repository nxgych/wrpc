#-*- coding: utf-8 -*-

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
        
    def __len__(self):
        return self.queue.qsize()    
    
    def _get_obj(self):
        return self.func(*self.args)
    
    def _close_obj(self, obj):
        if hasattr(obj, "close"):
            try:
                obj.close()  
            except:
                pass         
    
    def __put_obj(self, obj):
        if self.queue.qsize() < self.max_size:
            self.queue.put(obj) 
        else:
            self._close_obj(obj)
            del obj  
    
    def clear(self):
        with self._lock:
            while not self.queue.empty():
                obj = self.queue.get()
                self._close_obj(obj) 
                del obj  
            self.count = 0         
    
    def borrow_obj(self):
        with self._lock:
            if self.queue.empty() and self.count < self.queue.maxsize:
                self.queue.put(self._get_obj())
                self.count += 1
            return self.queue.get(True, self.wait_timeout) 
    
    def return_obj(self, obj):
        with self._lock:
            self.__put_obj(obj)    
            
    def destroy_obj(self, obj):
        with self._lock:
            self._close_obj(obj)
            del obj  
            if self.count > 0:
                self.count -= 1               

class KeyedObjectPool(ObjectPool):    
    """
    keyed object pool 
    """
    
    def __init__(self, func, *args, **kwargs):    
        super(KeyedObjectPool, self).__init__(func, *args, **kwargs)
        self.queue_map = {} # {key:[queue,count]}
        self.queue = None
        
    def __len__(self):
        return sum([v[0].qsize() for v in self.queue_map.itervalues()])    

    def __contains__(self, key):
        return key in self.queue_map
                
    def __getitem__(self, key):  
        return self.borrow_obj(key)     
    
    def __setitem__(self, key, obj):   
        self.return_obj(obj, key)   
            
    def __check(self, key):
        if key not in self.queue_map:
            self.queue_map[key] = [Queue.Queue(self.max_size), 0]
     
    def __put_obj(self, obj, key):
        self.__check(key)
        if self.queue_map[key][0].qsize() < self.max_size:
            self.queue_map[key][0].put(obj) 
        else:
            self._close_obj(obj) 
            del obj   

    def clear(self):
        with self._lock:
            for v in self.queue_map.itervalues():
                while not v[0].empty():
                    obj = v[0].get()
                    self._close_obj(obj) 
                    del obj  
                v[1]= 0  
                                
    def borrow_obj(self, key):
        with self._lock:
            self.__check(key)
            if (self.queue_map[key][0].empty() 
                and self.queue_map[key][1] < self.queue_map[key][0].maxsize):
                self.args = (key,)
                self.queue_map[key][0].put(self._get_obj())
                self.queue_map[key][1] += 1
            return self.queue_map[key][0].get(True, self.wait_timeout) 
    
    def return_obj(self, obj, key):
        with self._lock:
            self.__put_obj(obj, key) 

    def destroy_obj(self, obj, key):
        with self._lock:
            self._close_obj(obj) 
            del obj  
            if self.queue_map[key][1] > 0:
                self.queue_map[key][1] -= 1    
                