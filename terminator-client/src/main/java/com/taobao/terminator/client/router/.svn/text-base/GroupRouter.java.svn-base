package com.taobao.terminator.client.router;

import java.util.Map;

/**
 * 数据路由规则，数据有分组的时候，需要实现这个规则
 */
public interface GroupRouter {
	/**
	 * 根据传入的一行数据中的切分字段的值，返回对应的Shard number
	 */
	public String getGroupName(Map<String,String> rowData);
}
