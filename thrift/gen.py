#coding:utf-8

'''
Created on 2017年3月6日
@author: shuai.chen
'''

import os
import sys

Thrift = "thrift" # "/usr/local/bin/thrift" 
Languages = ["java", "python", "go"]

def gen_code(lan, model):
    """
    generate code
    @param lan: compute language
    @paran model: model file name 
    """
    lang = lan
    if "py" in lan:
        lang = "py"
        
    name = model
    if not model.endswith("thrift"):
        name = "%s.thrift" % name
  
    os.system("%s -r --gen %s thrift/%s" % (Thrift, lang, name))


if __name__ == "__main__":
    """
    generate appointed languages and appointed models
        command 'python gen.py java,python,go user,message'
    generate appointed languages and all models
        command 'python gen.py java,python,go'
    generate default languages and all models without arguments
        command 'python gen.py'    
    """
    args = sys.argv
    if len(args) < 2:
        for f in os.listdir("./thrift"):
            for lan in Languages:
                gen_code(lan, f)   
    elif len(args) == 2:   
        langs = args[1].split(",")
        for f in os.listdir("./thrift"):
            for lan in langs:
                gen_code(lan, f)                
    else:  
        langs = args[1].split(",")
        models = args[2].split(",")   
        for f in models:
            for lan in langs:
                gen_code(lan, f)
                
    print "finished!"
        
