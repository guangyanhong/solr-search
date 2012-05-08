package com.taobao.terminator.common.data.filter;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 用对sharkey对分组数取模的方式决定分组
 * @author liuyiding.pt
 */
public class ModGroupFilter extends AbstractGroupFilter {
	
	protected static Log logger = LogFactory.getLog(ModGroupFilter.class);

	@Override
	public boolean accept(Map<String, String> rowData) {
		if(!rowData.containsKey(shardKey)) {
			logger.error("给定数据中不包含shardKey为"+shardKey+"的参数");
			throw new IllegalArgumentException("给定数据中不包含shardKey为"+shardKey+"的参数");
		}
		String value = rowData.get(shardKey);
		try {
			return Long.parseLong(value) % groupNumber == shardNumber;
		} catch(NumberFormatException nfe) {
			logger.error("指定的shardKey的值无法转换成long类型");
			throw new IllegalArgumentException("指定的shardKey的值无法转换成long类型", nfe);
		}
	}
}
