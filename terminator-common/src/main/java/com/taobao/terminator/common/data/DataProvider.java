package com.taobao.terminator.common.data;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 数据获取的接口
 * 
 * @author yusen
 *
 */
public interface DataProvider {
	Log logger = LogFactory.getLog(DataProvider.class);

	/**
	 * 初始化操作，比如执行执行SQL等操作
	 * 
	 * @throws Exception
	 */
	public void init() throws Exception;

	/**
	 * 判断是否还有数据
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean hasNext() throws Exception;

	/**
	 * 获取一条记录
	 * 
	 * @return
	 * @throws Exception
	 */
	public Map<String, String> next() throws Exception;

	/**
	 * 释放资源，比如数据库连接，文件句柄等
	 * 
	 * @throws Exception
	 */
	public void close() throws Exception;
}
