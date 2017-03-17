package com.chen.wrpc.client;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;  
import java.lang.reflect.Method;  
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TServiceClient;  
import org.apache.thrift.TServiceClientFactory;
import org.springframework.beans.factory.FactoryBean;  
import org.springframework.beans.factory.InitializingBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chen.wrpc.client.ClientPoolFactory.PoolOperationCallBack;
import com.chen.wrpc.provider.ServerProvider;
  
/**
 * 客户端代理类, 待整理------
 * @author shuai.chen
 * @created 2016年10月31日
 */
public class ClientProxy implements FactoryBean<Object>, InitializingBean, Closeable {
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	volatile private static ClientProxy instance = null;  

	// 最大活跃连接数
	private Integer maxActive = 8;

	//ms,default 3 min,连接空闲时间,-1,关闭空闲检测
	private Integer idleTime = 180000;
	
	//server provicer
	private ServerProvider serverProvider;

	//inner attr
	private Map<String,Object> proxyClientMap = new HashMap<String,Object>();

	private ClientPool clientPool;

	private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	
	private PoolOperationCallBack callback = new PoolOperationCallBack() {
		@Override
		public void make(TServiceClient client) {
			logger.info("Client created.");
		}

		@Override
		public void destroy(TServiceClient client) {
			logger.info("Client destroy.");
		}
	};
	
	private ClientProxy(){}

	/**
	 * ClientProxy 单例
	 * @return ClientProxy object
	 */
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
		
	public void setMaxActive(Integer maxActive) {
		this.maxActive = maxActive;
	}

	public void setIdleTime(Integer idleTime) {
		this.idleTime = idleTime;
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
		// 加载Iface接口
        String[] interfaceNames = serverProvider.getServiceInterfaces();		        
        Class<?>[] objectClassArray = new Class[interfaceNames.length];
        Map<String,TServiceClientFactory<TServiceClient>> cfMap = new HashMap<String,TServiceClientFactory<TServiceClient>>();		
		
        for(int i=0; i<interfaceNames.length; i++){
        	String[] split = interfaceNames[i].split("\\.");
        	String serviceName = split[split.length-1];
        	
        	String interfaceName = interfaceNames[i] + "$Iface";
        	objectClassArray[i] = classLoader.loadClass(interfaceName);        

			// 加载Client.Factory类
        	String clientFactoryName = interfaceNames[i] + "$Client$Factory";
			@SuppressWarnings("unchecked")
			Class<TServiceClientFactory<TServiceClient>> fi = (Class<TServiceClientFactory<TServiceClient>>) classLoader.loadClass(clientFactoryName);
			TServiceClientFactory<TServiceClient> clientFactory = fi.newInstance();
			cfMap.put(serviceName, clientFactory);
        }
        
        setClientPool(cfMap);
        serverProvider.setClientPool(clientPool);

		for( Class<?> objectClass : objectClassArray){
			addClientProxy(objectClass);
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
        clientPool.setClientPoolFactory(new ClientPoolFactory(serverProvider, cfMap, callback));
        clientPool.setPoolConfig(maxActive, idleTime);
        clientPool.setPool();		
	}
	
	/**
	 * 添加客户端代理
	 * @param objectClass : array of interface class
	 */
	private void addClientProxy(Class<?> objectClass){	
		String[] split = objectClass.getName().split("\\.");
		final String key = split[split.length-1].split("\\$")[0];
		
		Object proxy = Proxy.newProxyInstance(classLoader, new Class[] { objectClass }, 
			new InvocationHandler() {
				@Override
				public Object invoke(Object proxy, Method method, Object[] args) 
						throws Throwable{
					TServiceClient client = clientPool.getPool().borrowObject(key);
					boolean flag = true;
					try {
						return method.invoke(client, args);
					} catch (Exception e) {
						flag = false;
						throw e;
					} finally {
						if(flag){
							clientPool.getPool().returnObject(key,client);
						}else{
							clientPool.getPool().invalidateObject(key,client);
						}
					}
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