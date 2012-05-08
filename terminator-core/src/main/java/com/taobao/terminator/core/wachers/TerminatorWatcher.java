package com.taobao.terminator.core.wachers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.Watcher;

import com.taobao.terminator.common.zk.TerminatorZkClient;
import com.taobao.terminator.core.ConfigControllor;

public abstract class TerminatorWatcher implements Watcher{
	
	protected static Log log = LogFactory.getLog(TerminatorWatcher.class);
	
	protected CoreContainer      coreContainer = null;
	protected ConfigControllor   configControllor  = null;
	protected TerminatorZkClient zkClient      = null;
	
	public TerminatorWatcher(TerminatorZkClient zkClient,CoreContainer coreContainer,ConfigControllor configControllor){
		this.zkClient      = zkClient;
		this.coreContainer = coreContainer;
		this.configControllor  = configControllor;
	}
	
	public TerminatorWatcher(TerminatorZkClient zkClient){
		this.zkClient = zkClient;
	}
	
	public CoreContainer getCoreContainer(){
		return coreContainer;
	}
	
	public void setCoreContainer(CoreContainer coreContainer){
		this.coreContainer = coreContainer;
	}
	
	public ConfigControllor getConfigControllor(){
		return configControllor;
	}
	
	public void setConfigControllor(ConfigControllor configControllor){
		this.configControllor = configControllor;
	}

	public TerminatorZkClient getZkClient() {
		return zkClient;
	}
	
	public void setZkClient(TerminatorZkClient zkClient){
		this.zkClient = zkClient;
	}
}
