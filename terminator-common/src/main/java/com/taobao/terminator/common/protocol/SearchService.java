package com.taobao.terminator.common.protocol;

import org.apache.solr.client.solrj.response.QueryResponse;

import com.taobao.terminator.common.TerminatorServiceException;

public interface SearchService {
	public QueryResponse query(TerminatorQueryRequest query) throws TerminatorServiceException;
}
