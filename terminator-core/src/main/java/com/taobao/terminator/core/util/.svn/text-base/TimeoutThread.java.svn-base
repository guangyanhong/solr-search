package com.taobao.terminator.core.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 该线程用来监听两次方法调用之间是否有超时。为了起到监听效果，客户端需要在有目标方法被调用的时候
 * 调用sleepAgain方法，然后中断TimeoutThread的实例。当TimeoutThread被中断之后，会判断是否需要
 * 继续sleep，如果需要继续sleep，那么就把isleepAgain的值设为false。这样如果客户端的目标方法被调用
 * 之后在超时时间之内没有被再次调用。当超过超时时间之后，TimeoutThread就回去执行handleTimeOut方法。
 * 
 * @author tianxiao
 * 
 */
public class TimeoutThread extends Thread {
	private static Log logger = LogFactory.getLog(TimeoutThread.class);
	private long waitingTime;
	private TimeoutHandler timeoutHandler;
	private String method;
	private AtomicBoolean isleepAgain;
	private AtomicBoolean isRunning;

	/**
	 * 构造函数
	 * 
	 * @param waitingTime
	 *            一次等待的时间
	 * @param timeoutHandler
	 *            TimeoutHandler
	 * @param method
	 *            当超时发生的时候代用的timeoutHandler中的方法。这个方法必须是无参数方法，因为
	 *            TimeoutThread使用反射来调用
	 */
	public TimeoutThread(long waitingTime, TimeoutHandler timeoutHandler,String method) {
		this.waitingTime = waitingTime;
		this.timeoutHandler = timeoutHandler;
		this.method = method;
		this.isleepAgain = new AtomicBoolean(false);
		this.isRunning = new AtomicBoolean(true);
	}

	public void run() {
		while (true) {
			try {
				Thread.sleep(this.waitingTime);
			} catch (InterruptedException e) {
				if (this.isRunning.get()) {
					if(this.isleepAgain.get()){
						logger.warn("等待超时时被中断，但是isleepAgain==true，说明要有新的调用在超时时间之前来了，这个超时线程被终止。。。");
						this.isleepAgain.set(false);
						continue;
					} else{
						logger.warn("等待超时时被中断，但是isleepAgain==false，说明在终止之前没有调用sleepAgain方法，为异常终止，直接退出，不调用handleTimeOut方法。");
						return;
					}
				} else {
					logger.warn("线程被中断，但是isRunning被设置成false，说明是正常退出，不调用handleTimeOut方法而正常退出。");
					return;
				}
			}
			this.handleTimeOut();
			break;
		}
	}

	public void sleepAgain() {
		this.isleepAgain.set(true);
	}

	/**
	 * 如果想要TimeoutThread正常推出而不去调用handleTimeOut方法，那么就需要在终止
	 * TimeoutThread之前调用一些stopRunning方法。
	 */
	public void stopRunning() {
		this.isRunning.set(false);
	}

	private void handleTimeOut() {
		try {
			Method handler = this.timeoutHandler.getClass().getDeclaredMethod(this.method);
			handler.setAccessible(true);
			handler.invoke(this.timeoutHandler);
		} catch (SecurityException e) {
			logger.error("无法调用TimeoutHandler的" + this.method + "方法", e);
		} catch (NoSuchMethodException e) {
			logger.error("无法在TimeoutHandler中找到方法" + this.method, e);
		} catch (IllegalArgumentException e) {
			logger.error("方法" + this.method + "必须是无参数的方法", e);
		} catch (IllegalAccessException e) {
			logger.error("无法访问TimeoutHandler的" + this.method + "方法", e);
		} catch (InvocationTargetException e) {
			logger.error("调用TimeoutHandler的" + this.method + "方法出现异常", e);
		}
	}
}
