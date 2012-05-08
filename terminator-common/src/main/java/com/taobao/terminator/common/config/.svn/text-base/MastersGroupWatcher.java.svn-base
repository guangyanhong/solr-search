package com.taobao.terminator.common.config;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.taobao.terminator.common.zk.TerminatorZKException;
import com.taobao.terminator.common.zk.TerminatorZkClient;

public class MastersGroupWatcher implements Watcher{
	private static Log log = LogFactory.getLog(GroupWatcher.class);

	private TerminatorZkClient zkClient = null;
	private GroupConfigSupport groupConfigSupport = null;
	private String groupName = null;

	public MastersGroupWatcher(TerminatorZkClient zkClient, GroupConfigSupport groupConfigSupport ,String groupName) {
		this.zkClient = zkClient;
		this.groupConfigSupport = groupConfigSupport;
		this.groupName = groupName;
	}

	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		EventType type = event.getType();
		log.warn("GroupConfig结构有变化，对应的zookeeper的znode的path ==> " + path + "EventType ==> " + type);
		if(type == EventType.NodeChildrenChanged){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				try {
					zkClient.getChildren(path,this);
				} catch (TerminatorZKException e) {
					log.error(e,e);
				}
				Thread.currentThread().interrupt();
			}
			List<String> hostList = null;
			try {
				if(!zkClient.exists(path)){
					log.warn("path ==> " + path + " 的节点不存在.");
					return ;
				}
				hostList = zkClient.getChildren(path, this);
			} catch (TerminatorZKException e) {
				log.error("获取path ==> " + path +" 的孩子节点失败,忽略此次改动.",e);
				return ;
			}
			
			if(hostList == null || hostList.isEmpty()){
				log.error("path ==>  " + path + "没有孩子节点，判断为误推送，忽略此次推送的信息.");
				return;
			}
			MastersGroupConfig groupConfig = new MastersGroupConfig(groupName);
			for (String hostInfo : hostList) {
				HostConfig hostConfig = HostInfoParser.toHostConfig(hostInfo);
				groupConfig.addHostConfig(hostConfig);
			}
			
			log.warn("调用GroupConfigSupport的setGroupConfig方法，以将变更通知到关注变更的对象.");
			groupConfigSupport.onGroupConfigChange(groupConfig);
		}else if(type == EventType.NodeDeleted){
			log.warn("path ==> " + path + "的znode节点被删除.");
		}
	}


}
