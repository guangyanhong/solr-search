package com.taobao.terminator.common.data.filter;

import java.util.Map;

/**
 * shard分组对数据进行过滤的类
 * @author liuyiding.pt
 */
public interface GroupFilter {
	/**
	 * 决定该行数据是否属于这个分组
	 * @param rowData 一行数据
	 * @return 是否属于这个分组
	 */
	public boolean accept(Map<String, String> rowData);
}
