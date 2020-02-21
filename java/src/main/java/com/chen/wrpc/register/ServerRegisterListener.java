package com.chen.wrpc.register;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chen.wrpc.common.WrpcException;
import com.chen.wrpc.ip.ServerIpResolve;
import com.chen.wrpc.server.ServerConfig;

/**
 * zookeeper连接监听
 * @author shuai.chen
 * @created 2017年3月2日
 */
public class ServerRegisterListener implements ConnectionStateListener{
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	private ServerConfig serverConfig;  
	private ServerIpResolve serverIpResolve;
  
    public ServerRegisterListener(ServerConfig serverConfig) {  
        this.serverConfig = serverConfig;
        this.serverIpResolve = serverConfig.getServerIpResolve();
    }  
  
    @Override  
    public void stateChanged(CuratorFramework zkClient, ConnectionState connectionState){  
        if(zkClient.getState() == CuratorFrameworkState.LATENT){  
            zkClient.start();  
        } 
	      
	    	switch(connectionState){
	    	    case CONNECTED:
		    		logger.info("Connection is successful!");
		    		break;
		    	case LOST:
		    		logger.info("Connection is lost!");
		    		break;
		    	case SUSPENDED:
		    		logger.info("Connection is suspended!");
		    		break;	    
		    	case RECONNECTED:
		    		logger.info("Connection is reconection!");
		    		break;		    		
		    	default:
		    		// do nothing	
	    	}
	    	//recreated any situation
        while(true){  
            try {  
            	synchronized(this){
	                if(zkClient.getZookeeperClient().blockUntilConnectedOrTimedOut()){  
	            		String path = "";
	            		if(serverConfig.getIp() != null){
	            			path = serverConfig.getPath();
	            		}else{	
	            			String oldPath = serverConfig.getPath();
	            			//reset server ip
	            			if(serverIpResolve != null)serverIpResolve.reset();
	            			path = serverConfig.getPath();
	            			//delete old path if ip changed
	            			if(!oldPath.equals(path)){
	                			if(zkClient.checkExists().forPath(oldPath) != null){
	                				zkClient.delete()
	                				.guaranteed()
	                				.deletingChildrenIfNeeded()
	                				.forPath(oldPath);
	                				logger.info("delete old path:" + oldPath);
	                			}
	            			}
	            		}
	            		
	            		logger.info("prelook register path:" + path);
	            		//create path
                		if(zkClient.checkExists().forPath(path) == null){
	                    	zkClient.create()
	                        .creatingParentsIfNeeded()
	                        .withMode(CreateMode.EPHEMERAL)
	                        .forPath(path);  
                		}
	            	}
                }
                break;   
            } catch (InterruptedException e) {  
            		logger.error("Register listener interrupted."); 
                break;  
            } catch (Exception e){ 
              	throw new WrpcException(e);
            }  
        }  
    } 

}
