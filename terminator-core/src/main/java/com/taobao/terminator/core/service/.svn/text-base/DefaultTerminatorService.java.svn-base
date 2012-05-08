package com.taobao.terminator.core.service;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

import com.taobao.terminator.common.CoreProperties;
import com.taobao.terminator.common.ServiceType;
import com.taobao.terminator.common.TerminatorHSFContainer;
import com.taobao.terminator.common.TerminatorHsfPubException;
import com.taobao.terminator.common.TerminatorServiceException;
import com.taobao.terminator.common.protocol.TerminatorQueryRequest;
import com.taobao.terminator.common.protocol.TerminatorService;

public class DefaultTerminatorService implements TerminatorService,NamedListInitializedPlugin{
	protected static Log log = LogFactory.getLog(TerminatorService.class);
	
	public static final int DEFAULT_HSF_TIME_OUT = 3000;
	
	protected CoreContainer      coreContainer      = null;
	protected SolrResourceLoader solrResourceLoader = null;
	protected EmbeddedSolrServer solrServer			= null;
	@SuppressWarnings("unchecked")
	protected NamedList 		 args				= null;
	
	protected CoreProperties coreProperties = null;
	
	protected String coreName = "";
	
	public DefaultTerminatorService(SolrCore solrCore){
		this.solrResourceLoader = solrCore.getResourceLoader();
		this.coreContainer      = solrCore.getCoreDescriptor().getCoreContainer();
		this.coreName           = solrCore.getName();
		this.solrServer     	= new EmbeddedSolrServer(coreContainer,coreName);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void init(NamedList args) {
		this.args = args;
		try {
			this.publishService();
		} catch (TerminatorHsfPubException e) {
			log.error("发布Hsf服务（reader,merger）时出错",e);
		}
	}
	
	public void publishService() throws TerminatorHsfPubException{
		try {
			coreProperties = new CoreProperties(solrResourceLoader.openConfig("core.properties"));
		} catch (IOException e) {
			RuntimeException re = new RuntimeException("读取 [" + coreName +"]的core.properties文件是出错 ",e);
			log.error(re,re);
			throw re;
		}
		
		boolean isReader = coreProperties.isReader();
		boolean isMerger = coreProperties.isMerger();
		
		String timeoutStr = (String)this.args.get("hsfTimeout");
		int timout = DEFAULT_HSF_TIME_OUT;
		
		try{
			timout = Integer.valueOf(timeoutStr);
		} catch(Exception e){}
		
		
		if(isReader){
			TerminatorHSFContainer.publishService(this, this.getInterfaceName(),TerminatorHSFContainer.Utils.genearteVersion(ServiceType.reader, coreName),timout);
		}
		
		if(isMerger){
			TerminatorHSFContainer.publishService(this, this.getInterfaceName(),TerminatorHSFContainer.Utils.genearteVersion(ServiceType.merger, coreName),timout);
		}
	}
	
	@Override
	public QueryResponse query(TerminatorQueryRequest query)throws TerminatorServiceException {
		long startTime = System.currentTimeMillis();
		QueryResponse resp = null;
		try {
			resp =  solrServer.query(query);
		} catch (SolrServerException e) {
			throw new TerminatorServiceException(e);
		}
		
		long totalTime = System.currentTimeMillis() - startTime;
		if(totalTime >= 3 * 1000){
			log.warn("Query时间过长，执行的Request ==> " + query.toString() + " 执行时间 ==> " + totalTime);
		}else if(log.isDebugEnabled()){
			log.debug("执行的Request ==> " + query.toString() + " 执行时间 ==> " + totalTime);
		}
		return resp;
	}
	
	private String getInterfaceName(){
		return TerminatorService.class.getName();
	}
	
	@Override
	public UpdateResponse add(Collection<SolrInputDocument> docs) throws TerminatorServiceException{
		try {
			return solrServer.add(docs);
		} catch (SolrServerException e) {
			throw new TerminatorServiceException(e);
		} catch (IOException e) {
			throw new TerminatorServiceException(e);
		}
	}

	@Override
	public UpdateResponse add(SolrInputDocument doc) throws TerminatorServiceException {
		try {
			return solrServer.add(doc);
		} catch (SolrServerException e) {
			throw new TerminatorServiceException(e);
		} catch (IOException e) {
			throw new TerminatorServiceException(e);
		}
	}

	@Override
	public UpdateResponse deleteById(String id)  throws TerminatorServiceException{
		try {
			return solrServer.deleteById(id);
		} catch (SolrServerException e) {
			throw new TerminatorServiceException(e);
		} catch (IOException e) {
			throw new TerminatorServiceException(e);
		}
	}

	@Override
	public UpdateResponse deleteById(List<String> ids)  throws TerminatorServiceException{
		try {
			return solrServer.deleteById(ids);
		} catch (SolrServerException e) {
			throw new TerminatorServiceException(e);
		} catch (IOException e) {
			throw new TerminatorServiceException(e);
		}
	}

	@Override
	public UpdateResponse deleteByQuery(String query)  throws TerminatorServiceException{
		try {
			return solrServer.deleteByQuery(query);
		} catch (SolrServerException e) {
			throw new TerminatorServiceException(e);
		} catch (IOException e) {
			throw new TerminatorServiceException(e);
		}
	}
	
	@Override
	public SolrPingResponse ping()  throws TerminatorServiceException{
		try {
			return solrServer.ping();
		} catch (SolrServerException e) {
			throw new TerminatorServiceException(e);
		} catch (IOException e) {
			throw new TerminatorServiceException(e);
		}
	}

	@Override
	public UpdateResponse optimize() throws TerminatorServiceException {
		throw new UnsupportedOperationException();
	}

	@Override
	public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher) throws TerminatorServiceException {
		throw new UnsupportedOperationException();
	}

	@Override
	public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher,
			int maxSegments)  throws TerminatorServiceException{
		throw new UnsupportedOperationException();
	}

	@Override
	public UpdateResponse rollback()  throws TerminatorServiceException{
		throw new UnsupportedOperationException();
	}
	
	@Override
	public UpdateResponse commit()  throws TerminatorServiceException{
		throw new UnsupportedOperationException();
	}

	@Override
	public UpdateResponse commit(boolean waitFlush, boolean waitSearcher)  throws TerminatorServiceException{
		throw new UnsupportedOperationException();
	}

	public CoreContainer getCoreContainer() {
		return coreContainer;
	}

	public void setCoreContainer(CoreContainer coreContainer) {
		this.coreContainer = coreContainer;
	}

	public SolrResourceLoader getSolrResourceLoader() {
		return solrResourceLoader;
	}

	public void setSolrResourceLoader(SolrResourceLoader solrResourceLoader) {
		this.solrResourceLoader = solrResourceLoader;
	}

	public EmbeddedSolrServer getSolrServer() {
		return solrServer;
	}

	public void setSolrServer(EmbeddedSolrServer solrServer) {
		this.solrServer = solrServer;
	}

	public CoreProperties getCoreProperties() {
		return coreProperties;
	}

	public void setCoreProperties(CoreProperties coreProperties) {
		this.coreProperties = coreProperties;
	}

	public String getCoreName() {
		return coreName;
	}

	public void setCoreName(String coreName) {
		this.coreName = coreName;
	}
}
