package com.chen.wrpc.register;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.CreateMode;

import com.chen.wrpc.common.WrpcException;
import com.chen.wrpc.server.ServerConfig;

/**
 * @author shuai.chen
 * @created 2017年2月22日
 */
public class ServerRegisterImpl implements ServerRegister,Closeable{

	private CuratorFramework zkClient; 

    public ServerRegisterImpl(){}  
    
    public ServerRegisterImpl(CuratorFramework zkClient){  
        this.zkClient = zkClient;  
    }  
	  
	@Override
	public void register(String path) {
		// TODO Auto-generated method stub
		  if(zkClient == null){
			  return;
		  }
		
	      if(zkClient.getState() == CuratorFrameworkState.LATENT){  
	          zkClient.start();  
	      }  
	      //临时节点  
	      try {  
	          zkClient.create()  
	              .creatingParentsIfNeeded()  
	              .withMode(CreateMode.EPHEMERAL)  
	              .forPath(path);  
	      } catch (UnsupportedEncodingException e) {  
	    	  throw new WrpcException("register service address to zookeeper exception: address UnsupportedEncodingException", e);
	      } catch (Exception e) {  
	    	  throw new WrpcException("register service address to zookeeper exception:{}", e);  
	      }  		
	}
	
	@Override
	public void registerAndListen(ServerConfig serverConfig){
		if(zkClient != null){
			zkClient.getConnectionStateListenable()
			.addListener(new ServerRegisterListener(serverConfig));
		}
	}

	public void close(){  
		if(zkClient != null){
			zkClient.close();  
		}
	}
	  
}
