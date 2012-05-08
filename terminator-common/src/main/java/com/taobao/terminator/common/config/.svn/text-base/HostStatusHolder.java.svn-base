package com.taobao.terminator.common.config;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.taobao.terminator.common.zk.NodeLifeManager;
import com.taobao.terminator.common.zk.TerminatorZKException;
import com.taobao.terminator.common.zk.TerminatorZkClient;

/**
 * 搜索服务对应的所有机器状态的Holder
 * 
 * @author yusen
 */
public class HostStatusHolder extends ConcurrentHashMap<String,Boolean>{
	private static Log log = LogFactory.getLog(HostStatusHolder.class);
	
	private static final long serialVersionUID = -1006468707809294518L;
	
	private ServiceConfig serviceConfig = null;
	private NodeLifeManager nodeLifeManager = null;
	
	public HostStatusHolder(TerminatorZkClient zkClient,ServiceConfig serviceConfig){
		this.serviceConfig = serviceConfig;
		this.nodeLifeManager = new NodeLifeManager(zkClient);
		this.initState();
	}
	
	public void initState(){
		Set<String> allIps = this.serviceConfig.getAllNodeIps();
		log.warn("初始化名为==>" + serviceConfig.getServiceName() +"的搜索服务的所有机器状态.");
		
		for(final String ip : allIps){
			boolean isAlive = false;
			try {
				isAlive = this.nodeLifeManager.isAlive(ip,new Watcher(){
					@Override
					public void process(WatchedEvent event) {
						
						String path = event.getPath();
						EventType type = event.getType();
						log.warn("机器节点可用状态变更，path ==> " + path +" EventType ==> " + type);
						
						if(event.getType() == EventType.NodeCreated){
							log.warn("机器节点可用状态变更 [" + ip + "] ===> true ,自此 此节点可被访问.");
							put(ip,true);
						}
						
						if(event.getType() == EventType.NodeDeleted){
							log.warn("机器节点可用状态变更 [" + ip + "] ===> false ,自此 此节点将不被访问.");
							put(ip,false);
						}
						
						log.warn("重新绑定对znode ==> " + path +" 的Watcher.");
						try {
							HostStatusHolder.this.nodeLifeManager.getZkClient().exists(event.getPath(),this);
						} catch (TerminatorZKException e) {
							log.error(e,e);
						}
					}
				});
			} catch (TerminatorZKException e) {
				log.error("获取ip为[" + ip +"] 的机器可用状态时出现异常，系统默认此机器为不可用状态",e);
			}
			this.put(ip, isAlive);
		}
	}
	
	/**
	 * 判断一台机器当前是否可用
	 * 
	 * @param ip
	 * @return
	 */
	public boolean isAlive(String ip){
		if(this.containsKey(ip)){
			return this.get(ip);
		}else{
			return false;
		}
	}

	public ServiceConfig getServiceConfig() {
		return serviceConfig;
	}

	public void setServiceConfig(ServiceConfig serviceConfig) {
		this.serviceConfig = serviceConfig;
	}

	public NodeLifeManager getNodeLifeManager() {
		return nodeLifeManager;
	}

	public void setNodeLifeManager(NodeLifeManager nodeLifeManager) {
		this.nodeLifeManager = nodeLifeManager;
	}
}
