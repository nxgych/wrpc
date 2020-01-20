package com.chen.wrpc.server;

import org.springframework.beans.factory.InitializingBean;

import com.chen.wrpc.common.Constant;
import com.chen.wrpc.ip.ServerIpResolve;
import com.chen.wrpc.ip.ServerIpResolveLocal;

/**
* @author shuai.chen
* @created 2017年2月22日
*/
public class ServerConfig implements InitializingBean{

	// 服务注册端口
	private Integer port = Constant.DEFAULT_PORT;
	//服务版本号
	private String version = Constant.DEFAULT_VERSION;	
	// 权重 优先级
	private Integer weight = Constant.DEFAULT_WEIGHT;

	//global service name
	private String globalService;
    // 实现类  
    private Object[] serviceImpls;

	//ip address if you force config it instead of getting local ip 
	private String ip;
	
	//inner attr
	private ServerIpResolve serverIpResolve;
	
    public ServerConfig(){}
    
    /**
     * get服务注册路径
     * @return 服务注册路径
     */
	public String getPath(){
		StringBuffer sb = new StringBuffer();
		sb.append(getParentPath());
		sb.append(Constant.ZK_SEPARATOR_DEFAULT);
		sb.append(getNodeName());
		return sb.toString();		
	}
	
	public String getParentPath(){
		StringBuffer sb = new StringBuffer();
		sb.append(Constant.ZK_SEPARATOR_DEFAULT);
		sb.append(globalService);
		sb.append(Constant.ZK_SEPARATOR_DEFAULT);
		sb.append(version);
		return sb.toString();	
	}
	
	/**
	 * 获取服务节点名称
	 * @return
	 */
	public String getNodeName(){
		if(serverIpResolve == null){
			this.setServerIpResolve();
		}
		StringBuffer sb = new StringBuffer();
		sb.append(serverIpResolve.getServerIp());
		sb.append(":");
		sb.append(port);
		sb.append(":");
		sb.append(weight);
		return sb.toString();
	}
	
	public Integer getPort() {
		return port;
	}
	public void setPort(Integer port) {
		this.port = port;
	}

	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}

	public String getGlobalService() {
		return globalService;
	}
	public void setGlobalService(String globalService) {
		this.globalService = globalService;
	}

	public Integer getWeight() {
		return weight;
	}
	public void setWeight(Integer weight) {
		this.weight = weight;
	}
	
	public Object[] getServiceImpls() {
		return serviceImpls;
	}
	public void setServiceImpls(Object[] serviceImpls) {
		this.serviceImpls = serviceImpls;
	}

	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	
	public void setServerIpResolve(){
		serverIpResolve = new ServerIpResolveLocal();
		serverIpResolve.setServerIp(ip);
	}
	
	public ServerIpResolve getServerIpResolve(){
		return serverIpResolve;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		// TODO Auto-generated method stub
		setServerIpResolve();
	}
    
}
