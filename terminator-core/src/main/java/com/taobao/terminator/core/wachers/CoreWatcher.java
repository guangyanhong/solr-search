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
		log.warn("·��Ϊ[" + path +"]��znode�ڵ㷢�����,EventTypeΪ [" + type+"]. ����Core�ڵ���д���Node�ڵ�ļ���������д����.");
	}
}
