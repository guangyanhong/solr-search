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
 * 监听 /terminator-nodes/192.168.211.29的znode节点，主要观察是否增加服务等
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
		log.warn("路径为[" + path +"]的znode节点发生变更,EventType为 [" + type+"].");
		
		if (EventType.NodeChildrenChanged == event.getType()) {// core有改动
			log.warn("本服务器提供的core发生了改变.");
			
			List<String> newCoreNameList = null;
			try {
				newCoreNameList = zkClient.getChildren(path,this);
			} catch (TerminatorZKException e) {
				log.error(e,e);
				return;
			}
			
			if(coreContainer == null){
				log.warn("SolrCore容器还未启动好，忽略此次node的变更.");
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
				log.warn("有新的Core的配置推送到本服务器.");
				for(String coreName : newAddCores){
					log.warn("\t 新增加Core[" + coreName +"],从ZK获取配置，并保存到本机FS,并加载该Core.");
					
					try {
						configControllor.fetchCore(coreName);
					} catch(Throwable e){
						log.error("\t获取coreName为[" + coreName + "]的配置文件出错.",e);
						continue;
					}
					
					CoreDescriptor dcore = new CoreDescriptor(coreContainer,coreName,coreName);
					SolrCore newCore = null;
					
					try {
						newCore = coreContainer.create(dcore);
					} catch (Throwable e) {
						log.error("\t创建新core出错 ==> " + coreName,e);
						log.warn("由于上述错异常，删除已经在本机文件系统保存的有问题的文件目录,并且删除ZK上对应的znode节点，忽略此新增的Core，不注册到CoreContainer.");
						try {
							configControllor.deleteCore(coreName);
							configControllor.deleteCoreFromZk(coreName);
						} catch (IOException e1) {
							log.error("删除SolrCore目录出出现异常",e1);
						} catch (TerminatorZKException e2) {
							log.error("删除ZK上coreName ==> " + coreName +" 的znode节点失败",e2);
						}
						continue;
					} 
					
					coreContainer.register(newCore, false);
					
					log.warn("加载并注册SolrCore [" + newCore.getName() +"] 的Terminator相关的Service.");
					MultiServiceContainer.getInstance().registerServiceContainer(new ServiceContainer(newCore));
				}
				needPersistent = true;
			}
			
			if(needDelCores != null && needDelCores.size() > 0){
				log.warn("本服务器需要卸载正在运行的Cores.");
				for(String coreName : needDelCores){
					log.warn("移除正在运行的Core，coreName为[" + coreName + "]");
					MultiServiceContainer.getInstance().getServiceContainer(coreName).unregisterAllServices();
					coreContainer.remove(coreName);
					try {
						configControllor.deleteCore(coreName);
					} catch (IOException e) {
						log.fatal("删除对应core[" + coreName + "] 配置文件失败.",e);
					}
				}
				log.warn(" ================> 由于目前HSF不支持服务卸载，故虽solrCore有卸载，但是该SolrCore对应的已经发布的HSF服务不能卸载，请通过重启该服务器的方式进行服务的卸载.");
				needPersistent = true;
			}
			
			if(needPersistent){
				coreContainer.persist();
			}
			
		} else if (EventType.NodeDataChanged == event.getType()) {// 节点的data发生变化
			try {
				String value = TerminatorZKUtils.toString(zkClient.getData(path,this));
				log.warn("路径为[" + path + "]的节点的data发生变更  ==>  [" + value +"]");
			} catch (TerminatorZKException e) {
				log.error(e,e);
			}
		} else {
			log.warn("对该事件类型不予处理，重新绑定watcher对象.");
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
		Set<String> addSet ; //新增
		Set<String> delSet ; //删除
		
		@SuppressWarnings("unused")
		Set<String> retainSet ; //保持不变的部分
	}
}
