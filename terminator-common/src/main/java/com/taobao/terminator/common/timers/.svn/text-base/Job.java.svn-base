package com.taobao.terminator.common.timers;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 任务描述对象，提供了befor，after的扩展，还有Exception的处理器JobExceptionHandler
 * 
 * @author yusen
 *
 */
public abstract class Job implements Runnable{
	private static JobExceptionHandler defaultHandler = new DiscardJobExceptionHandler();
	
	private JobExceptionHandler exceptionHandler = defaultHandler;
	private ReentrantLock lock = new ReentrantLock();
	
	public Job(){}
	public Job(JobExceptionHandler jobExceptionHandler) {
		this.exceptionHandler = jobExceptionHandler;
	}
	
	@Override
	public void run() {
		if(this.isRunning()) {
			throw new RejectedExecutionException("Job is running.....");
		}
		lock.lock();
		try {
			this.beforeJob();
			this.doJob();
			this.afterJob();
		} catch (Throwable e) {
			exceptionHandler.handleException(Thread.currentThread(), e);
		} finally {
			lock.unlock();
		}
	}
	
	public void beforeJob() { }
	
	public abstract void doJob() throws Throwable;
		
	public void afterJob() { }
	
	public boolean isRunning() {
		return lock.isLocked();
	}
	
	public JobExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}
	
	public void setExceptionHandler(JobExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}
	
	public static void setDefaultExceptionHandler(JobExceptionHandler exceptionHandler) {
		defaultHandler = exceptionHandler;
	}

	public static JobExceptionHandler getDefaultExceptionHandler() {
		return defaultHandler;
	}

	public interface JobExceptionHandler {
		public void handleException(Thread thread,Throwable e);
	}
	
	public static class DiscardJobExceptionHandler implements JobExceptionHandler {
		@Override
		public void handleException(Thread thread, Throwable e) {
			//do nothing
		}
	}
}
