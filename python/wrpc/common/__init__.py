#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Created on 2017年3月6日
@author: shuai.chen
'''

class WrpcException(Exception):
    
    def __init__(self, message):
        super(WrpcException,self).__init__()
        self.message = message
        
    def __str__(self):
        return self.message    
    
class HandlerException(Exception):  

    def __init__(self, class_name):
        super(HandlerException,self).__init__()
        self.class_name = class_name
        
    def __str__(self):
        return "'%s' should inherit relevant service Iface!" % self.class_name  
        
class UncheckedException(Exception):    
    
    def __init__(self, class_name):
        super(UncheckedException,self).__init__()
        self.class_name = class_name
        
    def __str__(self):
        return "'%s' is unchecked and can't be used at present!" % self.class_name      
    