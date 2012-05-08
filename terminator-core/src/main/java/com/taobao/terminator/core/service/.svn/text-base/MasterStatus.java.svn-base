package com.taobao.terminator.core.service;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.notify.utils.ConcurrentHashSet;

public class MasterStatus {
	
	public  AtomicBoolean started  = new AtomicBoolean(false); //开始全量
	public  AtomicBoolean dumping  = new AtomicBoolean(false); //正在Dump数据
	public  AtomicBoolean finished = new AtomicBoolean(false); //所有的客户端都已经调用了Finish方法，整个Dump数据结束了，单Index任务可能仍然在跑，这个标志为不代表整个索引构建完成，只是数据传送完毕而已
	
	public  AtomicBoolean indexing = new AtomicBoolean(false); 
	
	public  AtomicInteger remoteClienCount = new AtomicInteger(0);     //全量Dump的客户端的个数
	public  AtomicInteger slaveCount       = new AtomicInteger(0);
	
	public  ConcurrentHashSet<String> remoteClients = new ConcurrentHashSet<String>();

	public void reset(){
		started.set(false);
		dumping.set(false);
		finished.set(false);
		indexing.set(false);
		remoteClienCount.set(0);
		slaveCount.set(0);
		remoteClients.clear();
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		if(started.get()){
			sb.append("started --> ");
			if(dumping.get()){
				sb.append("dumping --> ");
				if(finished.get()){
					sb.append("finished");
				}
			}
		}
		
		sb.append(indexing.get()?"   isIndexing":"   notIndexing");
		sb.append("    ").append("remoteClientCount :").append(remoteClienCount.get()).append("    slaveCount :").append(slaveCount.get());
		sb.append("     remoteClients  :" + remoteClients);
		return sb.toString();
	}
}	
  