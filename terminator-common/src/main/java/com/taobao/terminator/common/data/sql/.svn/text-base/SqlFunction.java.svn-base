package com.taobao.terminator.common.data.sql;

/**
 * Sql中可以自定义函数,用$符号区分，自定义的函数会在SQL执行之前，根据Function的定义进行值的填充
 * 
 * @author yusen
 *
 */
public interface SqlFunction {
	
	/**
	 * 函数名称
	 * 
	 * @return
	 */
	public String getPlaceHolderName();
	
	/**
	 * 计算并返回对应函数的计算结果
	 * 
	 * @return
	 */
	public String getValue();
}
