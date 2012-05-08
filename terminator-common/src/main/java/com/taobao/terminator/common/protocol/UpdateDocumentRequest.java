package com.taobao.terminator.common.protocol;

import java.util.Map;

import org.apache.solr.common.SolrInputDocument;

public class UpdateDocumentRequest extends AddDocumentRequest{
	private static final long serialVersionUID = 3575026725555377881L;

	public UpdateDocumentRequest() {
		super();
	}

	public UpdateDocumentRequest(SolrInputDocument solrDoc, Map<String, String> routeValue) {
		super(solrDoc, routeValue);
	}
}
