package com.taobao.terminator.core.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ���߳������������η�������֮���Ƿ��г�ʱ��Ϊ���𵽼���Ч�����ͻ�����Ҫ����Ŀ�귽�������õ�ʱ��
 * ����sleepAgain������Ȼ���ж�TimeoutThread��ʵ������TimeoutThread���ж�֮�󣬻��ж��Ƿ���Ҫ
 * ����sleep�������Ҫ����sleep����ô�Ͱ�isleepAgain��ֵ��Ϊfalse����������ͻ��˵�Ŀ�귽��������
 * ֮���ڳ�ʱʱ��֮��û�б��ٴε��á���������ʱʱ��֮��TimeoutThread�ͻ�ȥִ��handleTimeOut������
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
	 * ���캯��
	 * 
	 * @param waitingTime
	 *            һ�εȴ���ʱ��
	 * @param timeoutHandler
	 *            TimeoutHandler
	 * @param method
	 *            ����ʱ������ʱ����õ�timeoutHandler�еķ�������������������޲�����������Ϊ
	 *            TimeoutThreadʹ�÷���������
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
						logger.warn("�ȴ���ʱʱ���жϣ�����isleepAgain==true��˵��Ҫ���µĵ����ڳ�ʱʱ��֮ǰ���ˣ������ʱ�̱߳���ֹ������");
						this.isleepAgain.set(false);
						continue;
					} else{
						logger.warn("�ȴ���ʱʱ���жϣ�����isleepAgain==false��˵������ֹ֮ǰû�е���sleepAgain������Ϊ�쳣��ֹ��ֱ���˳���������handleTimeOut������");
						return;
					}
				} else {
					logger.warn("�̱߳��жϣ�����isRunning�����ó�false��˵���������˳���������handleTimeOut�����������˳���");
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
	 * �����ҪTimeoutThread�����Ƴ�����ȥ����handleTimeOut��������ô����Ҫ����ֹ
	 * TimeoutThread֮ǰ����һЩstopRunning������
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
			logger.error("�޷�����TimeoutHandler��" + this.method + "����", e);
		} catch (NoSuchMethodException e) {
			logger.error("�޷���TimeoutHandler���ҵ�����" + this.method, e);
		} catch (IllegalArgumentException e) {
			logger.error("����" + this.method + "�������޲����ķ���", e);
		} catch (IllegalAccessException e) {
			logger.error("�޷�����TimeoutHandler��" + this.method + "����", e);
		} catch (InvocationTargetException e) {
			logger.error("����TimeoutHandler��" + this.method + "���������쳣", e);
		}
	}
}
