#coding:utf-8

'''
Created on 2017年3月6日
@author: shuai.chen
'''

class WrpcExcetion(Exception):
    
    def __init__(self, message):
        super(WrpcExcetion,self).__init__()
        self.message = message
        
    def __str__(self):
        return self.message    
    
class UncheckedException(Exception):    
    
    def __init__(self, class_name):
        super(UncheckedException,self).__init__()
        self.class_name = class_name
        
    def __str__(self):
        return "'%s' is unchecked and can't be used at present!" % self.class_name      
    