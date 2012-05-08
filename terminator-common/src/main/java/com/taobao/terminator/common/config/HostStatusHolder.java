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
 * ���������Ӧ�����л���״̬��Holder
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
		log.warn("��ʼ����Ϊ==>" + serviceConfig.getServiceName() +"��������������л���״̬.");
		
		for(final String ip : allIps){
			boolean isAlive = false;
			try {
				isAlive = this.nodeLifeManager.isAlive(ip,new Watcher(){
					@Override
					public void process(WatchedEvent event) {
						
						String path = event.getPath();
						EventType type = event.getType();
						log.warn("�����ڵ����״̬�����path ==> " + path +" EventType ==> " + type);
						
						if(event.getType() == EventType.NodeCreated){
							log.warn("�����ڵ����״̬��� [" + ip + "] ===> true ,�Դ� �˽ڵ�ɱ�����.");
							put(ip,true);
						}
						
						if(event.getType() == EventType.NodeDeleted){
							log.warn("�����ڵ����״̬��� [" + ip + "] ===> false ,�Դ� �˽ڵ㽫��������.");
							put(ip,false);
						}
						
						log.warn("���°󶨶�znode ==> " + path +" ��Watcher.");
						try {
							HostStatusHolder.this.nodeLifeManager.getZkClient().exists(event.getPath(),this);
						} catch (TerminatorZKException e) {
							log.error(e,e);
						}
					}
				});
			} catch (TerminatorZKException e) {
				log.error("��ȡipΪ[" + ip +"] �Ļ�������״̬ʱ�����쳣��ϵͳĬ�ϴ˻���Ϊ������״̬",e);
			}
			this.put(ip, isAlive);
		}
	}
	
	/**
	 * �ж�һ̨������ǰ�Ƿ����
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
