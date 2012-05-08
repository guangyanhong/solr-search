package com.taobao.terminator.web.perftest;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

public class RealTimeDataProducer extends Thread{
	Logger logger =  Logger.getLogger(RealTimeDataProducer.class.getName());
	
	private RealTimeDataProvider provider;
	private BlockingQueue<Object> bufferQueue;
	
	public RealTimeDataProducer(RealTimeDataProvider provider) {
		this.provider = provider;
		bufferQueue = new LinkedBlockingQueue<Object>(1000);
		this.setName("RealTimeDataProducer-Thread");
	}
	
	@Override
	public void run() {
		while(provider.hasNext()) {
			Object obj = provider.next();
			try {
				bufferQueue.put(obj);
			} catch (InterruptedException e) {
				logger.error("bufferQueue.put() ERROR",e);
			}
		}
	}
	
	public Object nextData() {
		try {
			return bufferQueue.take();
		} catch (InterruptedException e) {
			logger.error("bufferQueue.take() ERROR",e);
		}
		return null;
	}
}
