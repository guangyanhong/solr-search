package com.taobao.terminator.client.router;

import java.util.Map;

/**
 * ����·�ɹ��������з����ʱ����Ҫʵ���������
 */
public interface GroupRouter {
	/**
	 * ���ݴ����һ�������е��з��ֶε�ֵ�����ض�Ӧ��Shard number
	 */
	public String getGroupName(Map<String,String> rowData);
}
