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
 * ������������ڵ�ṹ��Ϣ���
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
		
		log.warn("ServiceConfig�ṹ�б仯����Ӧ��zookeeper��znode��path ==> " + path + "EventType ==> " + type);
		log.warn("���е�ServiceConfig�Ľṹ����:\n {\n" + serviceConfigSupport.getServiceConfig().toString() +"\n}\n");
		
		if(type == EventType.NodeChildrenChanged){
			log.warn("���¼���ServiceConfig����.");
			
			List<String> groupList = null;
			try {
				groupList = zkClient.getChildren(path,this);
			} catch (TerminatorZKException e) {
				log.error("��ȡpath ==> " + path +" �ĺ��ӽڵ�ʧ��,���Դ˴θĶ�.",e);
				return ;
			}
			
			if(groupList == null || groupList.isEmpty()){
				log.error("path ==>  " + path + "û�к��ӽڵ㣬�ж�Ϊ�����ͣ����Դ˴����͵���Ϣ.");
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
					log.error("��ȡpath ==> " + groupPath +" �ĺ��ӽڵ�ʧ��.",e);
					isOk = false;
				}
				for (String hostInfo : hostList) {
					HostConfig hostConfig = HostInfoParser.toHostConfig(hostInfo);
					groupConfig.addHostConfig(hostConfig);
				}
			}
			
			if(!isOk){
				log.error("���ڱ���znode�ڵ�����е��쳣���˴�������Ч.");
			}else{
				ServiceConfig serviceConfig = new ServiceConfig(serviceName);
				for(GroupConfig groupConfig : groupConfigList){
					serviceConfig.addGroupConfig(groupConfig);
				}
				serviceConfigSupport.onServiceConfigChange(serviceConfig);
				log.warn("�µ�ServiceConfig�Ľṹ����:\n {\n" + serviceConfig.toString() +"\n}\n");
			}
		}else{
			log.warn("��ʶ����¼����� path ==> " + path + "  ==> " + type + ",�����κδ������°�Watcher��������.");
			try {
				zkClient.getChildren(path,this);
			} catch (TerminatorZKException e) {
				log.error(e,e);
			}
		}
	}

}