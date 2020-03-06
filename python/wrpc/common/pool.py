#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Created on 2017年3月6日
@author: shuai.chen
'''

from queue import Queue
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
            pool_max_active_size : pool max active size, default is 4    
            pool_wait_timeout:  pool block time, default is None means forever          
        """
        super(ObjectPool, self).__init__()
        self.func = func
        self.args = args
        self.max_size = kwargs.get("pool_max_size", 8) #max size per key
        self.max_active_size = kwargs.get("pool_max_active_size", 4) #max active size per key
        self.wait_timeout = kwargs.get("pool_wait_timeout")
        
        #队列计数
        self.count = 0
        self.queue = Queue(self.max_size)
        
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
        with self._lock:
            if self.size() < self.max_size:
                self.queue.put(obj) 
            else:
                self._close_obj(obj)
                del obj  
            
    def size(self):
        return self.queue.qsize()        
    
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
        if self.size() < self.max_active_size:
            self.__put_obj(obj)    
        else:
            self.destroy_obj(obj)    
            
    def destroy_obj(self, obj):
        with self._lock:
            if obj:
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
        self.queue_map = {} # {key:{"queue":queue, "count":0}}
        self.queue = None
        
    def __len__(self):
        return sum([v["queue"].qsize() for v in self.queue_map.values()])    

    def __contains__(self, key):
        return key in self.queue_map
                
    def __getitem__(self, key):  
        return self.borrow_obj(key)     
    
    def __setitem__(self, key, obj):   
        self.return_obj(obj, key)   
            
    def __check(self, key):
        if key not in self.queue_map:
            self.queue_map[key] = {"queue":Queue(self.max_size), "count":0}
     
    def __put_obj(self, obj, key):
        with self._lock:
            self.__check(key)
            if self.size(key) < self.max_size:
                self.queue_map[key]["queue"].put(obj) 
            else:
                self._close_obj(obj) 
                del obj   

    def size(self, key):
        return self.queue_map[key]["queue"].qsize()

    def clear(self):
        with self._lock:
            for v in self.queue_map.values():
                while not v["queue"].empty():
                    obj = v["queue"].get()
                    self._close_obj(obj) 
                    del obj  
                v["count"]= 0  
                                
    def borrow_obj(self, key):
        with self._lock:
            self.__check(key)
            if (self.queue_map[key]["queue"].empty() 
                and self.queue_map[key]["count"] < self.queue_map[key]["queue"].maxsize):
                self.args = (key,)
                self.queue_map[key]["queue"].put(self._get_obj())
                self.queue_map[key]["count"] += 1
            return self.queue_map[key]["queue"].get(True, self.wait_timeout) 
    
    def return_obj(self, obj, key):
        if self.size(key) < self.max_active_size:
            self.__put_obj(obj, key) 
        else:
            self.destroy_obj(obj, key)    

    def destroy_obj(self, obj, key):
        with self._lock:
            if obj:
                self._close_obj(obj) 
                del obj  
                if self.queue_map[key]["count"] > 0:
                    self.queue_map[key]["count"] -= 1    
                