package com.taobao.terminator.common.protocol;

import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import com.taobao.terminator.common.TerminatorServiceException;

public interface TerminatorService {
	
	public QueryResponse query(TerminatorQueryRequest query) throws TerminatorServiceException;
	
	public UpdateResponse add(Collection<SolrInputDocument> docs)throws TerminatorServiceException;

	public UpdateResponse add(SolrInputDocument doc)throws TerminatorServiceException;

	public UpdateResponse commit()throws TerminatorServiceException;

	public UpdateResponse optimize()throws TerminatorServiceException;

	public UpdateResponse commit(boolean waitFlush, boolean waitSearcher)throws TerminatorServiceException;

	public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher)throws TerminatorServiceException;

	public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher, int maxSegments)throws TerminatorServiceException;

	public UpdateResponse rollback()throws TerminatorServiceException;

	public UpdateResponse deleteById(String id)throws TerminatorServiceException;

	public UpdateResponse deleteById(List<String> ids)throws TerminatorServiceException;

	public UpdateResponse deleteByQuery(String query)throws TerminatorServiceException;

	public SolrPingResponse ping()throws TerminatorServiceException;
}
