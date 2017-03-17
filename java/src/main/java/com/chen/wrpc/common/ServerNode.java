package com.chen.wrpc.common;

/**
 * @author shuai.chen
 * @created 2017年3月17日
 */
public class ServerNode {
	
	//ip
	private String ip;
	//port
	private Integer port;
	//weight
	private Integer weight;
	
	public ServerNode(String ip, Integer port, Integer weight) {
		super();
		this.ip = ip;
		this.port = port;
		this.weight = weight;
	}
	
	/**
	 * 初始化ServerNode
	 * @param address  string as 'ip:port:weight' or 'ip:port'
	 */
	public ServerNode(String address){
		String[] hostname = address.split(":");
		Integer weight = Constant.DEFAULT_WEIGHT;
		if (hostname.length == 3) {
			weight = Integer.valueOf(hostname[2]);
		}
		String ip = hostname[0];
		Integer port = Integer.valueOf(hostname[1]);
		
		this.ip = ip;
		this.port = port;
		this.weight = weight;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public Integer getWeight() {
		return weight;
	}

	public void setWeight(Integer weight) {
		this.weight = weight;
	}
	
}
