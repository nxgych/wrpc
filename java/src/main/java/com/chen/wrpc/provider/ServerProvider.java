package com.chen.wrpc.provider;

import java.util.List;

import com.chen.wrpc.client.ClientPool;
import com.chen.wrpc.common.ServerNode;

/**
 * @author shuai.chen
 * @created 2016年10月31日
 */
public interface ServerProvider {
	
    //获取接口
    String[] getServices();
  
    /** 
     * 获取所有服务端地址 
     * @return 
     */  
    List<ServerNode> findServerAddressList();  
  
    /** 
     * 选取一个合适的address,可以随机获取等
     * 内部可以使用合适的算法. 
     * @return 
     */  
    ServerNode selector();  
    
    /**
     * 设置连接池引用
     * @param clientPool
     */
    void setClientPool(ClientPool clientPool);
    
    //close object
    void close();
}
