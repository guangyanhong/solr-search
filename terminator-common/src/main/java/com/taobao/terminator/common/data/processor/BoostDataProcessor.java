package com.taobao.terminator.common.data.processor;

import java.util.Map;

/**
 * 设置Document的Boost参数的Processor<br>
 * 只需要实现抽象方法calculateBoost()即可，进行Boost的计算
 * @author yusen
 */
public abstract class BoostDataProcessor implements DataProcessor{
	public static final String BOOST_NAME = "!$boost";
	
	public abstract ResultCode process(Map<String, String> map);
	
	@Override
	public String getDesc() {
		return "设置Document的Boost参数的DataProcessor.";
	}

	/**
	 * 计算一个Document的Boost值
	 * 
	 * @param map
	 * @return
	 */
	protected void  setBoost(Map<String,String> map,float boost){
		map.put(BOOST_NAME, String.valueOf(boost));
	}
}
