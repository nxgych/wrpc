#coding:utf-8

'''
Created on 2017年4月1日
@author: shuai.chen
'''

import functools
from abc import ABCMeta, abstractmethod

class Proxy(object):
    """abstract proxy class"""
    
    __metaclass__ = ABCMeta
    
    def __getattr__(self, attr):
        return functools.partial(self.call, attr)
    
    @abstractmethod
    def call(self, fun, *args):
        raise NotImplementedError
    