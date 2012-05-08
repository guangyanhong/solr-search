/*package com.taobao.terminator.common.test;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.terminator.common.data.DataProvider;

*//**
 * DataProvider�����������
 * @author liuyiding.pt
 *//*
public abstract class DataProviderTestCase extends TestCase {
	
	protected static Log logger = LogFactory.getLog(DataProviderTestCase.class);
	
	protected DataProvider dataProvider;
	
	public final void setUp() {
		this.beforeSetUp();
		ApplicationContext applicationContext = new ClassPathXmlApplicationContext(getApplicationContextName());
		try {
			this.dataProvider = (DataProvider)applicationContext.getBean(this.getDataProviderName());
			this.dataProvider.init();
		} catch(BeansException be) {
			logger.error("��ȡָ����DataProviderʧ��", be);
		} catch (Exception e) {
			logger.error("dataProvider��ʼ��������ֱ�ӹر�", e);
			try {
				this.dataProvider.close();
			} catch(Exception e1) {
				logger.error("dataProvider�ر�ʱ����", e1);
			}
		}
	}
	
	public final void tearDown() {
		try {
			this.dataProvider.close();
		} catch(Exception e) {
			logger.error("dataProvider�ر�ʱ����", e);
		} finally {
			this.afterTearDown();
		}
	}
	
	*//**
	 * ��ÿ�����Է�������ǰ��Ҫ���õĶ���
	 *//*
	public abstract void beforeSetUp();
	
	*//**
	 * ��ÿ�����Է���������Ҫ����������
	 *//*
	public abstract void afterTearDown();
	
	*//**
	 * ��д������������spring�����ļ����ļ���
	 * @return spring�����ļ����ļ���
	 *//*
	public abstract String[] getApplicationContextName();
	
	*//**
	 * ��д������������DataProvider������
	 * @return spring�����ļ����ļ���
	 *//*
	public abstract String getDataProviderName();
}
*/