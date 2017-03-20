package com.chen.wrpc.provider;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.InitializingBean;

import com.chen.wrpc.client.ClientPool;
import com.chen.wrpc.common.ServerNode;
import com.chen.wrpc.loadbalance.LoadBalance;
import com.chen.wrpc.loadbalance.LoadBalanceRoundRobin;

/**
 * 通过配置获取服务地址
 */
public class ServerProviderFixed implements ServerProvider,InitializingBean{
	
	// 注册服务name
	private String globalService;
	// ip:port:weight
	private String serverAddress;
	//service interfaces
	private String[] serviceInterfaces;
	//load balance
	private LoadBalance loadBalance;
	
	//服务地址列表
	private final Set<ServerNode> container = new HashSet<ServerNode>();
    
    public ServerProviderFixed(){}

	@Override
	public List<ServerNode> findServerAddressList() {
		return Collections.unmodifiableList(loadBalance.getNodes());
	}

	@Override
	public ServerNode selector() {
		return loadBalance.getAddress();
	}
	
	@Override
	public void close() {
	}
	
	public String getGlobalService() {
		return globalService;
	}

	public void setGlobalService(String globalService) {
		this.globalService = globalService;
	}

	public String getServerAddress() {
		return serverAddress;
	}

	public void setServerAddress(String serverAddress) {
		this.serverAddress = serverAddress;
	}

	@Override
	public String[] getServiceInterfaces() {
		// TODO Auto-generated method stub
		return serviceInterfaces;
	}
	
	public void setServiceInterfaces(String[] serviceInterfaces) {
		this.serviceInterfaces = serviceInterfaces;
	}
	
	public void setLoadBalance(LoadBalance loadBalance){
		this.loadBalance = loadBalance;
	}
	public void setLoadBalance(){
		if(loadBalance == null){
			this.loadBalance = new LoadBalanceRoundRobin();
		}
	}
	
	@Override
	public void setClientPool(ClientPool clientPool){
	}
	
	public void setContainer(){
		String[] hostnames = serverAddress.split(",");
		Set<ServerNode> current = new HashSet<ServerNode>();
		for (String address : hostnames) {
			current.add(new ServerNode(address));
		}
		container.addAll(current);
		loadBalance.setNodes(container);
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		setLoadBalance();
		setContainer();
	}
	
}
