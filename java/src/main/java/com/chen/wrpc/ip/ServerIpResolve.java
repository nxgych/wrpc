package com.chen.wrpc.ip;

/**
 * @author shuai.chen
 * @created 2016年10月31日
 */
public interface ServerIpResolve {
	
    String getServerIp();  
    
    void setServerIp(String serverIp);
    
    void reset();  
      
    //当IP变更时,将会调用reset方法  
    static interface IpResetCalllBack{  
        public void reset(String newIp);  
    } 
}
