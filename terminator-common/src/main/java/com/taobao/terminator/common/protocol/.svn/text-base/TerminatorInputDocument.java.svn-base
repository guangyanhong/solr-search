package com.taobao.terminator.common.protocol;

import org.apache.solr.common.SolrInputDocument;

public class TerminatorInputDocument {
	private SolrInputDocument solrInputDocument;
	private Object routeValue = null;
	
	public TerminatorInputDocument(){}
	
	public TerminatorInputDocument(SolrInputDocument solrInputDocument){
		this(solrInputDocument,null);
	}

	public TerminatorInputDocument(SolrInputDocument solrInputDocument,Object routeValue){
		this.solrInputDocument = solrInputDocument;
		this.routeValue        = routeValue;
	}
	
	public SolrInputDocument getSolrInputDocument() {
		return solrInputDocument;
	}

	public void setSolrInputDocument(SolrInputDocument solrInputDocument) {
		this.solrInputDocument = solrInputDocument;
	}

	public Object getRouteValue() {
		return routeValue;
	}

	public void setRouteValue(Object routeValue) {
		this.routeValue = routeValue;
	}
}
