package com.taobao.terminator.common.protocol;

import java.io.Serializable;
import java.util.Map;

public class DeleteByIdRequest implements RouteValueSupport,Serializable {
	private static final long serialVersionUID = 2349500035462507700L;
	transient public Map<String,String> routeValue;
	public String id;
	
	public DeleteByIdRequest(){}
	
	public DeleteByIdRequest(Map<String, String> routeValue, String id) {
		super();
		this.routeValue = routeValue;
		this.id = id;
	}

	@Override
	public Map<String, String> getRouteValue() {
		return routeValue;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setRouteValue(Map<String, String> routeValue) {
		this.routeValue = routeValue;
	}
}
