package com.taobao.terminator.common.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.taobao.terminator.common.zk.TerminatorZKException;
import com.taobao.terminator.common.zk.TerminatorZKUtils;
import com.taobao.terminator.common.zk.TerminatorZkClient;

/**
 * 监听搜索服务节点结构信息变更
 * 
 * @author yusen
 */
public class ServiceWatcher implements Watcher{
	private static Log log = LogFactory.getLog(ServiceWatcher.class);
	
	private TerminatorZkClient zkClient = null;
	private String serviceName = null;
	private ServiceConfigSupport serviceConfigSupport;
	
	public ServiceWatcher(String serviceName,TerminatorZkClient zkClient,ServiceConfigSupport serviceConfigSupport){
		this.serviceName = serviceName;
		this.zkClient = zkClient;
		this.serviceConfigSupport = serviceConfigSupport;
		this.checkFields();
	}
	
	private void checkFields(){
	}

	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		EventType type = event.getType();
		
		log.warn("ServiceConfig结构有变化，对应的zookeeper的znode的path ==> " + path + "EventType ==> " + type);
		log.warn("现有的ServiceConfig的结构如下:\n {\n" + serviceConfigSupport.getServiceConfig().toString() +"\n}\n");
		
		if(type == EventType.NodeChildrenChanged){
			log.warn("重新加载ServiceConfig对象.");
			
			List<String> groupList = null;
			try {
				groupList = zkClient.getChildren(path,this);
			} catch (TerminatorZKException e) {
				log.error("获取path ==> " + path +" 的孩子节点失败,忽略此次改动.",e);
				return ;
			}
			
			if(groupList == null || groupList.isEmpty()){
				log.error("path ==>  " + path + "没有孩子节点，判断为误推送，忽略此次推送的信息.");
				return;
			}
			
			List<GroupConfig> groupConfigList = new ArrayList<GroupConfig>();
			boolean isOk = true;
			for (String groupName : groupList) {
				GroupConfig groupConfig = new GroupConfig(groupName);
				groupConfigList.add(groupConfig);
				
				String groupPath = TerminatorZKUtils.contactZnodePaths(path, groupName);
				List<String> hostList = null;
				try {
					Watcher groupWatcher = new GroupWatcher(zkClient, serviceConfigSupport.getServiceConfig(), groupName);
					hostList = zkClient.getChildren(groupPath,groupWatcher);
				} catch (TerminatorZKException e) {
					log.error("获取path ==> " + groupPath +" 的孩子节点失败.",e);
					isOk = false;
				}
				for (String hostInfo : hostList) {
					HostConfig hostConfig = HostInfoParser.toHostConfig(hostInfo);
					groupConfig.addHostConfig(hostConfig);
				}
			}
			
			if(!isOk){
				log.error("由于遍历znode节点过程中的异常，此次推送无效.");
			}else{
				ServiceConfig serviceConfig = new ServiceConfig(serviceName);
				for(GroupConfig groupConfig : groupConfigList){
					serviceConfig.addGroupConfig(groupConfig);
				}
				serviceConfigSupport.onServiceConfigChange(serviceConfig);
				log.warn("新的ServiceConfig的结构如下:\n {\n" + serviceConfig.toString() +"\n}\n");
			}
		}else{
			log.warn("不识别的事件类型 path ==> " + path + "  ==> " + type + ",不做任何处理，重新绑定Watcher监听对象.");
			try {
				zkClient.getChildren(path,this);
			} catch (TerminatorZKException e) {
				log.error(e,e);
			}
		}
	}

}