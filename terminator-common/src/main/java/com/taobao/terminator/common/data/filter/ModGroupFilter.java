package com.taobao.terminator.common.data.filter;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * �ö�sharkey�Է�����ȡģ�ķ�ʽ��������
 * @author liuyiding.pt
 */
public class ModGroupFilter extends AbstractGroupFilter {
	
	protected static Log logger = LogFactory.getLog(ModGroupFilter.class);

	@Override
	public boolean accept(Map<String, String> rowData) {
		if(!rowData.containsKey(shardKey)) {
			logger.error("���������в�����shardKeyΪ"+shardKey+"�Ĳ���");
			throw new IllegalArgumentException("���������в�����shardKeyΪ"+shardKey+"�Ĳ���");
		}
		String value = rowData.get(shardKey);
		try {
			return Long.parseLong(value) % groupNumber == shardNumber;
		} catch(NumberFormatException nfe) {
			logger.error("ָ����shardKey��ֵ�޷�ת����long����");
			throw new IllegalArgumentException("ָ����shardKey��ֵ�޷�ת����long����", nfe);
		}
	}
}
