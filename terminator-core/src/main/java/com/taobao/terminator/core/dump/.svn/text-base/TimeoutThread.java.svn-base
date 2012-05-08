package com.taobao.terminator.core.dump;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class TimeoutThread extends  Thread{
	
	protected Log logger = LogFactory.getLog(TimeoutThread.class);
	
	protected long   timeout   = 0L;
	protected long   startTime = 0L;
	protected boolean runnable = true;
	
	public TimeoutThread(long timeout){
		this.timeout   = timeout;
		this.startTime = System.currentTimeMillis();
		this.runnable  = true;
	}
	
	@Override
	public void run() {
		logger.warn("[Time-Out-Thread] 开启超时线程，TIME_OUT=[" + timeout + " ms]");
		while(runnable){
			try {
				Thread.sleep(60 * 1000);
			} catch (InterruptedException e) {
				Thread.interrupted();
			}
			if(System.currentTimeMillis() - startTime >= timeout){
				this.handleTimeoutEvent();
				return;
			}
		}
	}
	
	public void finish(){
		logger.warn("[Time-Out-Thread] 结束超时线程.");
		this.runnable = false;
	}
	
	public abstract void handleTimeoutEvent();
}
