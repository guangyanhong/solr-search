package com.taobao.terminator.core.wachers;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.taobao.terminator.common.zk.TerminatorZkClient;

public class CoreWatcher extends TerminatorWatcher{
	
	public CoreWatcher(TerminatorZkClient zkClient) {
		super(zkClient);
	}

	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		if(path == null){
			return;
		}
		EventType type = event.getType();
		log.warn("路径为[" + path +"]的znode节点发生变更,EventType为 [" + type+"]. 不对Core节点进行处理，Node节点的监听器会进行处理的.");
	}
}
