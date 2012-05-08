package com.taobao.terminator.client.index.data;

import java.util.List;

/**
 * 分表的描述规则解析
 * 
 * @author yusen
 *
 */
public interface SubtableDescParser {
	List<String> parse(String subtableDesc) throws SubtableDescParseException;
}
