package com.chen.wrpc.client;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;  
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TServiceClient;  
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.transport.TTransportException;
import org.springframework.beans.factory.FactoryBean;  
import org.springframework.beans.factory.InitializingBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chen.wrpc.common.WrpcException;
import com.chen.wrpc.provider.ServerProvider;
  
/**
 * 客户端代理类
 * @author shuai.chen
 * @created 2016年10月31日
 */
public class ClientProxy implements FactoryBean<Object>, InitializingBean, Closeable {
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	volatile private static ClientProxy instance = null;  

	// 最大连接数
	private Integer maxTotal = 8;
	// 最大活跃连接数
	private Integer maxActive = 4;

	//连接空闲时间 ms, default 3 min, -1:关闭空闲检测
	private Integer idleTime = 180000;
	
	//retry times
	private Integer retry = 3;
	
	//retry interval, default 200 ms
	private Integer retryInterval = 200;
	
	//server provicer
	private ServerProvider serverProvider;

	//inner attr
	private Map<String,Object> proxyClientMap = new HashMap<String,Object>();

	private ClientPool clientPool;

	private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	
	public ClientProxy(){}

	/**
	 * ClientProxy 单例
	 * @return ClientProxy object
	 */
	@Deprecated
    public static ClientProxy getInstance() {  
		if(instance != null){
			//do nothing
		}else{
			synchronized (ClientProxy.class) {  
			    if(instance == null){//二次检查  
				instance = new ClientProxy();  
			    }  
			} 
		}
	    return instance;  
    }
	
	public void setMaxTotal(Integer maxTotal) {
		this.maxTotal = maxTotal;
	}
	
	public void setMaxActive(Integer maxActive) {
		this.maxActive = maxActive;
	}

	public void setIdleTime(Integer idleTime) {
		this.idleTime = idleTime;
	}
	
	public void setRetry(Integer retry){
		this.retry = retry;
	}
	
	public void setRetryInterval(Integer retryInterval){
		this.retryInterval = retryInterval;
	}

	public void setServerProvider(ServerProvider serverProvider) {
		this.serverProvider = serverProvider;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		loadClientProxy();
		Thread.sleep(1000);
	}
	
	/**
	 * 加载客户端代理
	 * @throws Exception
	 */
	public void loadClientProxy() throws Exception{
        String[] interfaceNames = serverProvider.getServices();		        
        Map<String,TServiceClientFactory<TServiceClient>> cfMap = new HashMap<String,TServiceClientFactory<TServiceClient>>();		
		
        // 生成Client.Factory Map
        for(int i=0; i<interfaceNames.length; i++){
        	String[] split = interfaceNames[i].split("\\.");
        	String serviceName = split[split.length-1];   

			// 加载Client.Factory类
        	String clientFactoryName = interfaceNames[i] + "$Client$Factory";
			@SuppressWarnings("unchecked")
			Class<TServiceClientFactory<TServiceClient>> fi = (Class<TServiceClientFactory<TServiceClient>>) classLoader.loadClass(clientFactoryName);
			TServiceClientFactory<TServiceClient> clientFactory = fi.newInstance();
			cfMap.put(serviceName, clientFactory);
        }
        
        // set client pool
        setClientPool(cfMap);
        serverProvider.setClientPool(clientPool);
		
        //添加客户端代理
		for(int i=0; i<interfaceNames.length; i++){
	        	String[] split = interfaceNames[i].split("\\.");
	        	String serviceName = split[split.length-1];
	        	
	        	String interfaceName = interfaceNames[i] + "$Iface";
	        	Class<?> objectClass = classLoader.loadClass(interfaceName);    
	        	
	        	addClientProxy(serviceName, objectClass);
		}
	}
	
	/**
	 * set client pool
	 * @param cfMap  {service name : client class}
	 * @throws Exception
	 */
	private void setClientPool(Map<String,TServiceClientFactory<TServiceClient>> cfMap) 
			throws Exception{
        clientPool = new ClientPool();
        clientPool.setClientFactory(new ClientFactory(serverProvider, cfMap));
        clientPool.setPoolConfig(maxTotal, maxActive, idleTime);
        clientPool.setPool();		
	}
	
	/**
	 * 添加客户端代理
	 * @param key : serviceName
	 * @param objectClass : array of interface class
	 */
	private void addClientProxy(final String key, Class<?> objectClass){	
		Object proxy = Proxy.newProxyInstance(classLoader, new Class[] {objectClass}, 
			new InvocationHandler() {
				@Override
				public Object invoke(Object proxy, Method method, Object[] args) 
						throws Throwable{
					Throwable exception = null;

					for(int i=0; i < retry; i++){
						TServiceClient client = null;
						boolean flag = true;
						try {
							client = clientPool.getPool().borrowObject(key);
							return method.invoke(client, args);
						} catch (TTransportException | InvocationTargetException e){
							exception = e;
							flag = false;
							logger.error("Could not connect server!");
						} catch (Throwable e) {
							exception = e;
							flag = false;
						} finally {
							if(client != null){
								if(flag){
									clientPool.getPool().returnObject(key,client);
								}else{
									clientPool.getPool().invalidateObject(key,client);
								}
							}
						}
						Thread.sleep(retryInterval);
					}
					throw new WrpcException(exception);
				}
			}
		);
		proxyClientMap.put(key, proxy);
	}

	@Override
	public Class<?> getObjectType() {
		return TServiceClient.class;
	}
	
	@Override
	public Object getObject() throws Exception {
		return proxyClientMap;
	}
	
	/**
	 * get client
	 * @param key : service name
	 * @return client object
	 */
	public Object getClient(String key){
		return proxyClientMap.get(key);
	}

	/**
	 * get client
	 * @param key : service class
	 * @return client object
	 */
	public Object getClient(Class<?> clazz){
		String key = clazz.getSimpleName();
		return proxyClientMap.get(key);
	}

	@Override
	public boolean isSingleton() {
		return true;
	}
	
	@Override
	public void close() throws IOException {
		if(clientPool != null){
			clientPool.close();
		}
		if (serverProvider != null) {
			serverProvider.close();
		}
	}
	
}  
