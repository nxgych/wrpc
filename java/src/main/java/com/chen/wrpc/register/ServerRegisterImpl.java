package com.chen.wrpc.register;

import java.io.Closeable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chen.wrpc.server.Server;
import com.chen.wrpc.server.ServerConfig;
import com.chen.wrpc.common.WrpcException;

/**
 * @author shuai.chen
 * @created 2017年2月22日
 */
public class ServerRegisterImpl implements ServerRegister,Closeable{

	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private Server server; 

    public ServerRegisterImpl(){}  
    
    public ServerRegisterImpl(Server server){  
        this.server = server;  
    }  
	  
	@Override
	public void register() {
		// TODO Auto-generated method stub
		while(true){  
            try {  
            		synchronized(this){
            			CuratorFramework zkClient = server.getZkClient();
            			ServerConfig serverConfig = server.getServerConfig();
            			
	                if(zkClient.getZookeeperClient().blockUntilConnectedOrTimedOut()){  
		            		String path = "";
		            		if(serverConfig.getIp() != null){
		            			path = serverConfig.getPath();
		            		}else{	
		            			String oldPath = serverConfig.getPath();
		            			//reset server ip
		            			serverConfig.getServerIpResolve().reset();
		            			path = serverConfig.getPath();
		            			//delete old path if local ip changed
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
		                    	logger.info("registe server success.");
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
	
	@Override
	public void registerAndListen(){
		CuratorFramework zkClient = server.getZkClient();		
		if(zkClient != null){
			zkClient.getConnectionStateListenable()
			.addListener(new ConnectionStateListener() {
				
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
				    	register();
			    } 

			});
		}
	}

	public void close(){  
		if(server != null){
			server.close();  
		}
	}
	  
}
