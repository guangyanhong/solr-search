package com.taobao.terminator.client.index.data;

import java.util.List;

/**
 * �ֱ�������������
 * 
 * @author yusen
 *
 */
public interface SubtableDescParser {
	List<String> parse(String subtableDesc) throws SubtableDescParseException;
}
