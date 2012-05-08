package com.taobao.terminator.common.zk;

import org.apache.zookeeper.Watcher;

import com.taobao.terminator.common.TerminatorCommonUtils;

/**
 * @author yusen
 */
public class NodeLifeManager {
	private TerminatorZkClient zkClient = null;
	
	public NodeLifeManager(TerminatorZkClient zkClient){
		this.zkClient = zkClient;
	}
	
	public void registerSelf() throws TerminatorZKException{
		String localIp = TerminatorCommonUtils.getLocalHostIP();
		zkClient.createPath(TerminatorZKUtils.getNodeStatusPath(localIp), false);
	}
	
	public boolean isAlive() throws TerminatorZKException{
		String path = TerminatorZKUtils.getNodeStatusPath(TerminatorCommonUtils.getLocalHostIP());
		return zkClient.exists(path);
	}
	
	public boolean isAlive(String hostIP)throws TerminatorZKException{
		String path = TerminatorZKUtils.getNodeStatusPath(hostIP);
		return zkClient.exists(path);
	}
	
	public boolean isAlive(String hostIP,Watcher watcher)throws TerminatorZKException{
		String path = TerminatorZKUtils.getNodeStatusPath(hostIP);
		return zkClient.exists(path,watcher);
	}

	public TerminatorZkClient getZkClient() {
		return zkClient;
	}

	public void setZkClient(TerminatorZkClient zkClient) {
		this.zkClient = zkClient;
	}
}
