package com.taobao.terminator.core.wachers;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.zk.TerminatorZKException;
import com.taobao.terminator.common.zk.TerminatorZKUtils;
import com.taobao.terminator.common.zk.TerminatorZkClient;
import com.taobao.terminator.core.ConfigControllor;
import com.taobao.terminator.core.service.MultiServiceContainer;
import com.taobao.terminator.core.service.ServiceContainer;

/**
 * ���� /terminator-nodes/192.168.211.29��znode�ڵ㣬��Ҫ�۲��Ƿ����ӷ����
 * 
 * @author yusen
 */
public class NodeWatcher extends TerminatorWatcher {
	
	
	public NodeWatcher(TerminatorZkClient zkClient,CoreContainer coreContainer, ConfigControllor configControllor) {
		super(zkClient, coreContainer, configControllor);
	}
	
	public NodeWatcher(TerminatorZkClient zkClient){
		super(zkClient);
	}

	@Override
	public void process(WatchedEvent event) {
		
		String path = event.getPath();
		if(path == null){
			return;
		}
		EventType type =event.getType();
		log.warn("·��Ϊ[" + path +"]��znode�ڵ㷢�����,EventTypeΪ [" + type+"].");
		
		if (EventType.NodeChildrenChanged == event.getType()) {// core�иĶ�
			log.warn("���������ṩ��core�����˸ı�.");
			
			List<String> newCoreNameList = null;
			try {
				newCoreNameList = zkClient.getChildren(path,this);
			} catch (TerminatorZKException e) {
				log.error(e,e);
				return;
			}
			
			if(coreContainer == null){
				log.warn("SolrCore������δ�����ã����Դ˴�node�ı��.");
				return ;
			}
			
			Collection<String> oldCoreNameList = coreContainer.getCoreNames();
			
			Set<String> newCoreNameSet = this.processChildrens(newCoreNameList);
			Set<String> oldCoreNameSet = new HashSet<String>(oldCoreNameList);
			
			CoreChangeResult result = this.analyzeChange(oldCoreNameSet, newCoreNameSet);
			Set<String> newAddCores = result.addSet;
			Set<String> needDelCores = result.delSet;
			
			boolean needPersistent = false;
			
			if(newAddCores != null && newAddCores.size() > 0){
				log.warn("���µ�Core���������͵���������.");
				for(String coreName : newAddCores){
					log.warn("\t ������Core[" + coreName +"],��ZK��ȡ���ã������浽����FS,�����ظ�Core.");
					
					try {
						configControllor.fetchCore(coreName);
					} catch(Throwable e){
						log.error("\t��ȡcoreNameΪ[" + coreName + "]�������ļ�����.",e);
						continue;
					}
					
					CoreDescriptor dcore = new CoreDescriptor(coreContainer,coreName,coreName);
					SolrCore newCore = null;
					
					try {
						newCore = coreContainer.create(dcore);
					} catch (Throwable e) {
						log.error("\t������core���� ==> " + coreName,e);
						log.warn("�����������쳣��ɾ���Ѿ��ڱ����ļ�ϵͳ�������������ļ�Ŀ¼,����ɾ��ZK�϶�Ӧ��znode�ڵ㣬���Դ�������Core����ע�ᵽCoreContainer.");
						try {
							configControllor.deleteCore(coreName);
							configControllor.deleteCoreFromZk(coreName);
						} catch (IOException e1) {
							log.error("ɾ��SolrCoreĿ¼�������쳣",e1);
						} catch (TerminatorZKException e2) {
							log.error("ɾ��ZK��coreName ==> " + coreName +" ��znode�ڵ�ʧ��",e2);
						}
						continue;
					} 
					
					coreContainer.register(newCore, false);
					
					log.warn("���ز�ע��SolrCore [" + newCore.getName() +"] ��Terminator��ص�Service.");
					MultiServiceContainer.getInstance().registerServiceContainer(new ServiceContainer(newCore));
				}
				needPersistent = true;
			}
			
			if(needDelCores != null && needDelCores.size() > 0){
				log.warn("����������Ҫж���������е�Cores.");
				for(String coreName : needDelCores){
					log.warn("�Ƴ��������е�Core��coreNameΪ[" + coreName + "]");
					MultiServiceContainer.getInstance().getServiceContainer(coreName).unregisterAllServices();
					coreContainer.remove(coreName);
					try {
						configControllor.deleteCore(coreName);
					} catch (IOException e) {
						log.fatal("ɾ����Ӧcore[" + coreName + "] �����ļ�ʧ��.",e);
					}
				}
				log.warn(" ================> ����ĿǰHSF��֧�ַ���ж�أ�����solrCore��ж�أ����Ǹ�SolrCore��Ӧ���Ѿ�������HSF������ж�أ���ͨ�������÷������ķ�ʽ���з����ж��.");
				needPersistent = true;
			}
			
			if(needPersistent){
				coreContainer.persist();
			}
			
		} else if (EventType.NodeDataChanged == event.getType()) {// �ڵ��data�����仯
			try {
				String value = TerminatorZKUtils.toString(zkClient.getData(path,this));
				log.warn("·��Ϊ[" + path + "]�Ľڵ��data�������  ==>  [" + value +"]");
			} catch (TerminatorZKException e) {
				log.error(e,e);
			}
		} else {
			log.warn("�Ը��¼����Ͳ��账�����°�watcher����.");
			try {
				zkClient.getData(path,this);
				zkClient.getChildren(path,this);
			} catch (TerminatorZKException e) {
				log.warn(e,e);
			}
		}
	}
	
	private Set<String> processChildrens(List<String> children){
		Set<String> set = new HashSet<String>();
		for(String name : children){
			if(TerminatorCommonUtils.isCorrectCoreName(name)){
				set.add(name);
			}
		}
		return set;
	}
	
	private CoreChangeResult analyzeChange(Set<String> oldCoreNameSet,Set<String> newCoreNameSet){
		Set<String> intersection = new HashSet<String>();
		
		for (String e : newCoreNameSet) {
			if (oldCoreNameSet.contains(e)) {
				intersection.add(e);
			}
		}
		
		if (intersection.size() >= 0) {
			newCoreNameSet.removeAll(intersection);
			oldCoreNameSet.removeAll(intersection);
		}
		
		CoreChangeResult result = new CoreChangeResult();
		result.addSet = newCoreNameSet;
		result.delSet = oldCoreNameSet;
		result.retainSet = intersection;
		
		return result;
	}
	
	private class CoreChangeResult{
		Set<String> addSet ; //����
		Set<String> delSet ; //ɾ��
		
		@SuppressWarnings("unused")
		Set<String> retainSet ; //���ֲ���Ĳ���
	}
}
