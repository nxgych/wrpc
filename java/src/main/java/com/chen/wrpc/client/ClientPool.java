package com.chen.wrpc.client;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chen.wrpc.common.WrpcException;

/**
 * client pool object
 * @author shuai.chen
 * @created 2017年3月2日
 */
public class ClientPool implements Closeable{
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	//client pool
	private GenericKeyedObjectPool<String, TServiceClient> pool;
	//pool config
	private GenericKeyedObjectPoolConfig poolConfig;
	//client pool factory
	private ClientFactory clientFactory;
	
	public ClientPool(){}
	
	public GenericKeyedObjectPool<String, TServiceClient> getPool() {
		return pool;
	}

	public void setPool(GenericKeyedObjectPool<String, TServiceClient> pool) {
		this.pool = pool;
	}
	
	public void setPool(){
		pool = new GenericKeyedObjectPool<String, TServiceClient>(clientFactory, poolConfig);			
	}

	public GenericKeyedObjectPoolConfig getPoolConfig() {
		return poolConfig;
	}

	public void setPoolConfig(GenericKeyedObjectPoolConfig poolConfig) {
		this.poolConfig = poolConfig;
	}

	/**
	 * 设置连接池配置，简化配置 : 只支持最大活跃数和空闲时间配置
	 * @param maxActive 最大活跃数
	 * @param idleTime 空闲时间
	 */
	public void setPoolConfig(Integer maxTotal, Integer maxActive, Integer idleTime){
		GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();
        poolConfig.setMaxTotalPerKey(maxTotal);  
        poolConfig.setMaxIdlePerKey(maxActive); 
        poolConfig.setMinEvictableIdleTimeMillis(idleTime); 
        poolConfig.setTimeBetweenEvictionRunsMillis(idleTime / 2L);	
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(false);
        poolConfig.setTestWhileIdle(false);		
        this.poolConfig = poolConfig;
	}
	
	public ClientFactory getClientFactory() {
		return clientFactory;
	}

	public void setClientFactory(ClientFactory clientFactory) {
		this.clientFactory = clientFactory;
	}

	public synchronized void clearPool(){
		pool.clear();
		logger.info("Client pool cleared.");
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if(pool!=null){
			try {
				pool.close();
			} catch (Exception e) {
				throw new WrpcException(e); 
			}
		}		
	}
	
}
