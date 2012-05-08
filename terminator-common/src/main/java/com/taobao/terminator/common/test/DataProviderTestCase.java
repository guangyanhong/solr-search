/*package com.taobao.terminator.common.test;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.terminator.common.data.DataProvider;

*//**
 * DataProvider抽象测试用例
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
			logger.error("获取指定的DataProvider失败", be);
		} catch (Exception e) {
			logger.error("dataProvider初始化出错，将直接关闭", e);
			try {
				this.dataProvider.close();
			} catch(Exception e1) {
				logger.error("dataProvider关闭时出错", e1);
			}
		}
	}
	
	public final void tearDown() {
		try {
			this.dataProvider.close();
		} catch(Exception e) {
			logger.error("dataProvider关闭时出错", e);
		} finally {
			this.afterTearDown();
		}
	}
	
	*//**
	 * 在每个测试方法调用前需要设置的东西
	 *//*
	public abstract void beforeSetUp();
	
	*//**
	 * 在每个测试方法调用需要做的清理工作
	 *//*
	public abstract void afterTearDown();
	
	*//**
	 * 重写本方法，返回spring配置文件的文件名
	 * @return spring配置文件的文件名
	 *//*
	public abstract String[] getApplicationContextName();
	
	*//**
	 * 重写本方法，返回DataProvider的名字
	 * @return spring配置文件的文件名
	 *//*
	public abstract String getDataProviderName();
}
*/