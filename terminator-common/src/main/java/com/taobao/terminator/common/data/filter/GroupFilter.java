package com.taobao.terminator.common.data.filter;

import java.util.Map;

/**
 * shard��������ݽ��й��˵���
 * @author liuyiding.pt
 */
public interface GroupFilter {
	/**
	 * �������������Ƿ������������
	 * @param rowData һ������
	 * @return �Ƿ������������
	 */
	public boolean accept(Map<String, String> rowData);
}
