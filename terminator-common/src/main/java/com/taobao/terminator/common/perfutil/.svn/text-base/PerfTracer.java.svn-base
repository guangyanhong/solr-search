package com.taobao.terminator.common.perfutil;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;

public class PerfTracer {
	private AtomicInteger counter;
	private long startTime;
	private long intervalTime = 1 * 60 * 1000;
	private Log logger;
	private String name;
	
	public PerfTracer(String name,Log logger) {
		this.logger = logger;
		this.name = name;
		this.startTime = System.currentTimeMillis();
		this.counter = new AtomicInteger(0);
	}
	
	public void increment() {
		counter.incrementAndGet();
		if((System.currentTimeMillis() - startTime) >= intervalTime){
			this.onTime();
		}
	}
	
	public void reset() {
		counter.set(0);
		this.startTime = System.currentTimeMillis();
	}
	
	protected String exportLog() {
		return "[" + name+"-statistics] " + (counter.get() / (intervalTime/1000)) + "-tps";
	}
	
	protected void onTime() {
		logger.warn(this.exportLog());
		this.reset();
	}
}
