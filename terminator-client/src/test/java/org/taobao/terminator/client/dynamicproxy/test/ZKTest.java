package org.taobao.terminator.client.dynamicproxy.test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZKTest {
	public static void main(String[] args) throws Exception {
		final CountDownLatch l = new CountDownLatch(1);
		Watcher w = new Watcher(){

			@Override
			public void process(WatchedEvent event) {
				l.countDown();
			}
		};
		
		ZooKeeper zk = new ZooKeeper("192.168.211.29:2181", 3000, w);
		l.await();
		
		zk.exists("/aa/bb", new Watcher(){
			@Override
			public void process(WatchedEvent event) {
				
			}
		});
	}
}	
