package com.taobao.terminator.common.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.CommonParams;

public class TerminatorQueryRequest extends SolrQuery {
	private static final long serialVersionUID = -5750475678394136675L;
	private List<Map<String, String>> routeValues = null;

	/**
	 * 添加用来路由的值，由于可能有由多个字段才能路由到具体的某一个shard
	 * 故此处路由的值用Map对象，此处的路由字段会有GroupRouter类进行具体的路由 操作
	 * 
	 * @param value
	 * @return
	 */
	public TerminatorQueryRequest addRouteValue(Map<String, String> value) {
		if (routeValues == null) {
			routeValues = new ArrayList<Map<String, String>>();
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

	public List<Map<String, String>> getRouteValues() {
		return this.routeValues;
	}

	public void setRouteValues(List<Map<String, String>> routeValues) {
		this.routeValues = routeValues;
	}

	public SolrQuery addRangeQuery(String... rq) {
		this.add("rq", rq);
		return this;
	}

	public String[] getRangeQuery() {
		return this.getParams("rq");
	}
}
