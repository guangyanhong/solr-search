package com.taobao.terminator.common.data.filter;

import java.util.Map;

/**
 * ����groupNumber��shardKey�ĳ��������
 * @author liuyiding.pt
 */
public abstract class AbstractGroupFilter implements GroupFilter {
	
	protected int groupNumber;
	protected String shardKey;
	protected int shardNumber;
	
	public int getGroupNumber() {
		return groupNumber;
	}

	public void setGroupNumber(int groupNumber) {
		this.groupNumber = groupNumber;
	}

	public String getShardKey() {
		return shardKey;
	}

	public void setShardKey(String shardKey) {
		this.shardKey = shardKey;
	}
	
	public int getShardNumber() {
		return shardNumber;
	}

	public void setShardNumber(int shardNumber) {
		this.shardNumber = shardNumber;
	}

	public abstract boolean accept(Map<String, String> rowData);
}
