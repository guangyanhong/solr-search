package com.taobao.terminator.common.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TerminatorDeleteRequest {
	private String id = null;
	private String query = null;

	private List<Map<String, Object>> routeValues = null;

	/**
	 * 添加用来路由的值，由于可能有由多个字段才能路由到具体的某一个shard
	 * 故此处路由的值用Map对象，此处的路由字段会有GroupRouter类进行具体的路由 操作
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
	 * 判断是否包含有路由字段
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
