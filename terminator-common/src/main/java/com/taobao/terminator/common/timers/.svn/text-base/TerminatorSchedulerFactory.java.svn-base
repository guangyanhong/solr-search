package com.taobao.terminator.common.timers;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class TerminatorSchedulerFactory {

	public static ScheduledThreadPoolExecutor newScheduler(int corePoolSize, ThreadFactory threadFactory) {
		if(threadFactory == null)  {
			threadFactory = new DefaultThreadFactory();
		}
		
		if(corePoolSize < 0 ) {
			corePoolSize = 1;
		}
		
		ScheduledThreadPoolExecutor s = new ScheduledThreadPoolExecutor(corePoolSize, threadFactory);
		s.setRejectedExecutionHandler(new NewThreadRunsPolicy());
		return s;
	}

	public static ScheduledThreadPoolExecutor newScheduler() {
		return newScheduler(1, new DefaultThreadFactory());
	}

	/**
	 * 默认的ThreadFatctory
	 * 
	 * @author yusen
	 *
	 */
	public static class DefaultThreadFactory implements ThreadFactory {
		static final AtomicInteger poolNumber = new AtomicInteger(1);
		final ThreadGroup group;
		final AtomicInteger threadNumber = new AtomicInteger(1);
		final String namePrefix;

		public DefaultThreadFactory() {
			SecurityManager s = System.getSecurityManager();
			group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
			namePrefix = "Terminator-Scheduler-" + poolNumber.getAndIncrement() + "-Thread-";
		}

		public Thread newThread(Runnable r) {
			Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
			if (t.isDaemon())
				t.setDaemon(false);
			if (t.getPriority() != Thread.NORM_PRIORITY)
				t.setPriority(Thread.NORM_PRIORITY);
			return t;
		}
	}

	/**
	 * 当线程池中的线程不够用的时候临时创建一个新的Thread，临时用一下
	 * 
	 * @author yusen
	 *
	 */
	public static final class NewThreadRunsPolicy implements RejectedExecutionHandler {
		public NewThreadRunsPolicy() {
			super();
		}

		public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
			try {
				final Thread t = new Thread(r, "Terminator-Scheduler-Temporary-Thread");
				t.start();
			} catch (Throwable e) {
				throw new RejectedExecutionException("Failed to start a new thread", e);
			}
		}
	}
}
