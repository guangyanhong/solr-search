package com.taobao.terminator.common.data;

import java.util.List;
import java.util.Map;

/**
 * Sql执行器
 */
public interface SqlExecutor {
	/**
	 * 初始化方法，再次可获取连接，做SQL执行前的准备工作
	 * @throws Exception
	 */
	public void init() throws Exception;
	
	/**
	 * 执行SQL
	 * 
	 * @param param  在SQL中用占位符替代的参数
	 * @return 
	 * @throws Exception
	 */
	public List<Map<String, String>> execute(Map<String, String> param) throws Exception;
	
	/**
	 * 释放数据库连接等资源
	 * 
	 * @throws Exception
	 */
	public void close() throws Exception;
}
