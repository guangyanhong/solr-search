package com.taobao.terminator.common.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TerminatorDeleteRequest {
	private String id = null;
	private String query = null;

	private List<Map<String, Object>> routeValues = null;

	/**
	 * �������·�ɵ�ֵ�����ڿ������ɶ���ֶβ���·�ɵ������ĳһ��shard
	 * �ʴ˴�·�ɵ�ֵ��Map���󣬴˴���·���ֶλ���GroupRouter����о����·�� ����
	 * 
	 * @param value
	 * @return
	 */
	public TerminatorDeleteRequest addRouteValue(Map<String, Object> value) {
		if (routeValues == null) {
			routeValues = new ArrayList<Map<String, Object>>();
		}
		routeValues.add(value);
		return this;
	}

	/**
	 * �ж��Ƿ������·���ֶ�
	 * 
	 * @return
	 */
	public boolean containsRouteValues() {
		return routeValues != null && !routeValues.isEmpty();
	}

	public List<Map<String, Object>> getRouteValues() {
		return this.routeValues;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public void setRouteValues(List<Map<String, Object>> routeValues) {
		this.routeValues = routeValues;
	}
}
