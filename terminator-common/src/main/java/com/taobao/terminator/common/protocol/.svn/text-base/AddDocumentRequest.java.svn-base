package com.taobao.terminator.common.protocol;

import java.io.Serializable;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;

public class AddDocumentRequest implements RouteValueSupport, Serializable{
	private static final long serialVersionUID = -3948440619130257371L;

	public SolrInputDocument solrDoc;
	
	transient public Map<String,String> routeValue;

	public AddDocumentRequest(){}
	
	public AddDocumentRequest(SolrInputDocument solrDoc, Map<String, String> routeValue) {
		super();
		this.solrDoc = solrDoc;
		this.routeValue = routeValue;
	}

	@Override
	public Map<String, String> getRouteValue() {
		return routeValue;
	}

	public SolrInputDocument getSolrDoc() {
		return solrDoc;
	}

	public void setSolrDoc(SolrInputDocument solrDoc) {
		this.solrDoc = solrDoc;
	}

	public void setRouteValue(Map<String, String> routeValue) {
		this.routeValue = routeValue;
	}
}
