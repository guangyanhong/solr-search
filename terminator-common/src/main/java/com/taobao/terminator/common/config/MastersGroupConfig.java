package com.taobao.terminator.common.config;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.taobao.terminator.common.zk.TerminatorZKException;
import com.taobao.terminator.common.zk.TerminatorZKUtils;
import com.taobao.terminator.common.zk.TerminatorZkClient;

public class MastersGroupConfig extends GroupConfig{
	private static final long serialVersionUID = -7365409626720708663L;

	private TerminatorZkClient zkClient;
	private String serviceName;
	private GroupConfigSupport groupConfigSupport = null;
	private CountDownLatch latch  = null;
	
	public MastersGroupConfig(String groupName){
		super(groupName);
	}
	
	public MastersGroupConfig(String serviceName, String groupName,TerminatorZkClient zkClient,GroupConfigSupport groupConfigSupport) throws TerminatorZKException {
		super(serviceName);
		this.groupName = groupName;
		this.serviceName = serviceName;
		this.zkClient = zkClient;
		this.groupConfigSupport = groupConfigSupport;
		this.initConfig();
	}
	
	private void initConfig() throws TerminatorZKException{
		log.warn("初始换GroupConfig对象.");
		String groupPath = TerminatorZKUtils.contactZnodePaths(TerminatorZKUtils.getMainPath(serviceName),groupName);
		/* *
		 * 可能配置文件数发布了，然后Node监听到了有新的Core添加到该Node节点，此时会加载该Core并通过获取正向的Main-Tree信息发布或者
		 * 订阅相应的HSF服务，但是由于发布配置文件树和发布Main-Tree不是一个原子操作，导致代码跑到这个地方Main-Tree还没有发布好，故
		 * 需要再此等待一段时间，在等待的时间内如果还未发布相应的Main-Tree，则直接报异常
		 * 
		 * */
		boolean exists = zkClient.exists(groupPath,new Watcher(){
			@Override
			public void process(WatchedEvent event) {
				if(event.getType() == EventType.NodeCreated){
					latch.countDown();
				}
			}
		});
		
		if(!exists){
			log.warn("相应的正向Main-Tree信息还未发布，稍微等待一会儿. path ==> " + groupPath);
			try {
				latch = new CountDownLatch(1);
				exists = latch.await(60 * 1,TimeUnit.SECONDS); //等待10分钟
			} catch (InterruptedException e) {
				log.error("CountDownLatch.await()失败",e);
			}
		}
		
		if(!exists){
			throw new TerminatorZKException("ZooKeeper上没有path ==> " + groupPath + "的znode结点，请检查serviceName ==> " + serviceName +" 是否正确.");
		}
			
		Watcher groupWatcher = new MastersGroupWatcher(zkClient,groupConfigSupport,groupName);
		
		List<String> hostList = zkClient.getChildren(groupPath,groupWatcher);
		for (String hostInfo : hostList) {
			HostConfig hostConfig = HostInfoParser.toHostConfig(hostInfo);
			this.addHostConfig(hostConfig);
		}
	}
}
