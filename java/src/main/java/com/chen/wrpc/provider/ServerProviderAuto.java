package com.chen.wrpc.provider;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.beans.factory.InitializingBean;

import com.chen.wrpc.client.ClientPool;
import com.chen.wrpc.common.Constant;
import com.chen.wrpc.common.ServerNode;
import com.chen.wrpc.common.WrpcException;
import com.chen.wrpc.loadbalance.LoadBalance;
import com.chen.wrpc.loadbalance.LoadBalanceRoundRobin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 PathChildrenCache 的服务提供类
 * @author shuai.chen
 * @created 2016年10月31日
 */
public class ServerProviderAuto implements ServerProvider, InitializingBean ,Closeable{

	private Logger logger = LoggerFactory.getLogger(getClass());

	private CountDownLatch countDownLatch= new CountDownLatch(1);
	
	// 服务注册name
	private String globalServiceName = "";
	// 服务版本号
	private String version = Constant.DEFAULT_VERSION;
    //zookeeper client
	private CuratorFramework zkClient;
	//service interfaces
	private String[] services;
	//loadbalance
	private LoadBalance loadBalance;
	
	//inner attr
	private ClientPool clientPool; 
	private PathChildrenCache cachedPath;

	// 用来保存当前provider所接触过的地址记录
	// 当zookeeper集群故障时,可以使用trace中地址,作为"备份"
	private Map<String, ServerNode> trace = new HashMap<String, ServerNode>();
	
	//服务地址容器
	private final Set<ServerNode> container = new HashSet<ServerNode>();

	private Object lock = new Object();

	public ServerProviderAuto(){}
	
	public void setGlobalServiceName(String globalServiceName) {
		this.globalServiceName = globalServiceName;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public void setZkClient(CuratorFramework zkClient) {
		this.zkClient = zkClient;
	}

	public void setServices(String[] services) {
		this.services = services;
	}

	public void setLoadBalance(LoadBalance loadBalance){
		this.loadBalance = loadBalance;
	}
	public void setLoadBalance(){
		if(loadBalance == null){
			this.loadBalance = new LoadBalanceRoundRobin();
		}
	}
	
	public void setClientPool(ClientPool clientPool){
		this.clientPool = clientPool;
	}

	public String getGlobalServiceName() {
		return globalServiceName;
	}
	
	@Override
	public String[] getServices() {
		return services;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		setLoadBalance();
		startPathCache();
	}

	/**
	 * 启动ChildrenPathCache
	 * @throws Exception
	 */
	public void startPathCache() throws Exception{
		if(zkClient == null){
			return;
		}
		// 如果zk尚未启动,则启动
		if (zkClient.getState() == CuratorFrameworkState.LATENT) {
			zkClient.start();
		}
		buildPathChildrenCache(zkClient, getParentPath(), true);
		cachedPath.start(StartMode.POST_INITIALIZED_EVENT);
		countDownLatch.await();		
	}
	
	/**
	 * 获取服务地址的父级路径
	 * @return
	 */
	private String getParentPath(){
		StringBuffer sb = new StringBuffer();
		sb.append(Constant.ZK_SEPARATOR_DEFAULT);
		if(globalServiceName != ""){
			sb.append(globalServiceName);
			sb.append(Constant.ZK_SEPARATOR_DEFAULT);
		}
		sb.append(version);
		return sb.toString();	
	}
	
	/**
	 * path children listen
	 * @param client : zookeeper client
	 * @param path : parent path
	 * @param cacheData
	 * @throws Exception
	 */
	private void buildPathChildrenCache(final CuratorFramework client, String path, Boolean cacheData) throws Exception {
		try{
			cachedPath = new PathChildrenCache(client, path, cacheData);
			
			cachedPath.getListenable().addListener(new PathChildrenCacheListener() {
				
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) 
						throws Exception {
					PathChildrenCacheEvent.Type eventType = event.getType();
					switch (eventType) {
						case CONNECTION_RECONNECTED:
							logger.info("PathChildrenCache is reconection.");
							break;
						case CONNECTION_SUSPENDED:
							logger.info("PathChildrenCache is suspended.");
							break;
						case CONNECTION_LOST:
							logger.warn("PathChildrenCache is lost.");
							break;
						case INITIALIZED:
							logger.info("PathChildrenCache init ...");
							break;
	                    case CHILD_ADDED: 
		                    	logger.info("Child node added.");
		                    	break;
	                    case CHILD_UPDATED: 
		                    	logger.info("Child node updated.");
		                    	break;
	                    case CHILD_REMOVED: 
		                    	logger.info("Child node removed.");	
		                    	break;
						default:
							// do nothing
					}
					// 任何节点数据变动,都会rebuild,此处为一个"简单的"做法.
					try{
						if(client.getZookeeperClient().blockUntilConnectedOrTimedOut()){
							cachedPath.rebuild();
							rebuild();
							//update client proxy pool
							if (clientPool != null)clientPool.clearPool();
						}
					}catch(Exception e){
						throw e;
					}
				}
	
				protected void rebuild() throws Exception {
					List<ChildData> children = cachedPath.getCurrentData();
					if (children == null || children.isEmpty()) {
						container.clear();
						logger.error("Server not found!");
						return;
					}
					
					Set<ServerNode> current = new HashSet<ServerNode>();
					String path = null;
					for (ChildData data : children) {
						path = data.getPath();
						path = path.substring(getParentPath().length()+1);
						String address = new String(path.getBytes(), "utf-8");
						
						ServerNode serverNode = new ServerNode(address);
						current.add(serverNode);
						trace.put(address, serverNode);
					}
					
					synchronized (lock) {
						container.clear();
						container.addAll(current);
						loadBalance.setNodes(container);
					}
				}
				
			});
		}catch(Exception e){
			throw e;
		}finally{
			countDownLatch.countDown();
		}
	}

	@Override
	public List<ServerNode> findServerAddressList() {
		return Collections.unmodifiableList(loadBalance.getNodes());
	}

	@Override
	public ServerNode selector() {
		if (container.isEmpty()) {
		    if (!trace.isEmpty()) {
				synchronized (lock) {
					for (ServerNode node : trace.values()) {
						container.add(node);
					}
					loadBalance.setNodes(container);
				}
		    }
		}	
		return loadBalance.getAddress();
	}
	
	@Override
	public void close() {
		try {
			if(cachedPath != null){
				cachedPath.close();
			}
			if(zkClient != null){
				zkClient.close();
			}
        } catch (Exception e) {
        		throw new WrpcException(e);
        }
	}
	
}  