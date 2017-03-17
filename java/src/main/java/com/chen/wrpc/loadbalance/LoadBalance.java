package com.chen.wrpc.loadbalance;

//import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.chen.wrpc.common.ServerNode;

/**
 * @author shuai.chen
 * @created 2017年3月8日
 */
public abstract class LoadBalance{

	//服务地址列表
	protected List<ServerNode> nodes;

	//获取服务地址
	abstract public ServerNode getAddress();

	/**
	 * 设置服务节点
	 * @param container 服务容器
	 */
	public void setNodes(Set<ServerNode> container){
		this.nodes = transfer(container);
	}
	
	public List<ServerNode> getNodes(){
		return nodes;
	}

	/**
	 * ServerNode转换
	 * @return list
	 */
	protected List<ServerNode> transfer(Set<ServerNode> container) {
		List<ServerNode> result = new ArrayList<ServerNode>();
		for(ServerNode serverNode : container){
			// 根据优先级，将serverNode添加多次到地址集中
			for (int i = 0; i < serverNode.getWeight(); i++) {
				result.add(serverNode);
			}
		}
		Collections.shuffle(result);
		return result;
	}
	
}
