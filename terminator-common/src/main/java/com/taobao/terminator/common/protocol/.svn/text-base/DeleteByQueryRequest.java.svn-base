package com.taobao.terminator.common.protocol;

import java.io.Serializable;
import java.util.Map;

public class DeleteByQueryRequest implements RouteValueSupport, Serializable {
	private static final long serialVersionUID = 2347117955060292621L;
	
	transient public Map<String,String> routeValue;
	public String query;
	
	public DeleteByQueryRequest(){}
	
	public DeleteByQueryRequest(Map<String, String> routeValue, String query) {
		super();
		this.routeValue = routeValue;
		this.query = query;
	}

	@Override
	public Map<String, String> getRouteValue() {
		return routeValue;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public void setRouteValue(Map<String, String> routeValue) {
		this.routeValue = routeValue;
	}
}
