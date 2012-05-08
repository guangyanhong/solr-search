package com.taobao.terminator.core.realtime;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.TerminatorConstant;
import com.taobao.terminator.common.TerminatorServiceException;
import com.taobao.terminator.common.protocol.AddDocumentRequest;
import com.taobao.terminator.common.protocol.Address;
import com.taobao.terminator.common.protocol.DeleteByIdRequest;
import com.taobao.terminator.common.protocol.DeleteByQueryRequest;
import com.taobao.terminator.common.protocol.RealTimeService;
import com.taobao.terminator.common.protocol.UpdateDocumentRequest;
import com.taobao.terminator.core.dump.SolrCommandBuilder;

/**
 * 直接写内存索引，不做CommitLog复制，前期测试用，线上服务不推荐使用
 * 
 * @author yusen
 */
@Deprecated
public class DirectRAMIndexRealTimeService implements RealTimeService ,SelfPublisher{
	protected static Log log = LogFactory.getLog(DirectRAMIndexRealTimeService.class);
	
	protected SolrCore solrCore;
	protected SolrServer solrServer;
	
	public DirectRAMIndexRealTimeService(){}
	
	public DirectRAMIndexRealTimeService(SolrCore solrCore){
		this.solrCore = solrCore;
		this.solrServer = new EmbeddedSolrServer(solrCore.getCoreDescriptor().getCoreContainer(),solrCore.getName());
	}

	@Override
	public void publishHsfService(String coreName) {
		String version = null;
        //TODO
//		HSFSpringProviderBean providerBean = new HSFSpringProviderBean();
//		providerBean.setTarget(this);
//		providerBean.setServiceInterface(RealTimeService.class.getName());
//		providerBean.setServiceVersion(version = solrCore.getName() + "-" + TerminatorCommonUtils.getLocalHostIP());
//		providerBean.setSerializeType(TerminatorConstant.DEFAULT_SERIALIZE_TYPE);
//		providerBean.setClientTimeout(3000);
//
//		try {
//			log.error("Publish RealTimeService ,hsfVersion is {" + version + "}");
//			providerBean.afterPropertiesSet();
//		} catch (Exception e) {
//			log.error("Publish RealTimeService Error",e);
//		}
		
	}
	
	@Override
	public int add(AddDocumentRequest addReq) throws TerminatorServiceException {
		AddUpdateCommand addCmd = new AddUpdateCommand();
		addCmd.allowDups = true;
		addCmd.solrDoc = addReq.solrDoc;
		addCmd.doc = SolrCommandBuilder.getLuceneDocument(addReq.solrDoc, solrCore.getSchema());
		try {
			return this.solrCore.getUpdateHandler().addDoc(addCmd);
		} catch (IOException e) {
			throw new TerminatorServiceException("add.error", e);
		}
	}
	
	@Override
	public int update(UpdateDocumentRequest updateReq) throws TerminatorServiceException {
		AddUpdateCommand addCmd = new AddUpdateCommand();
		addCmd.allowDups = false;
		addCmd.solrDoc = updateReq.solrDoc;
		addCmd.doc = SolrCommandBuilder.getLuceneDocument(updateReq.solrDoc, solrCore.getSchema());
		try {
			return this.solrCore.getUpdateHandler().addDoc(addCmd);
		} catch (IOException e) {
			throw new TerminatorServiceException("add.error", e);
		}
	}

	@Override
	public int delete(DeleteByIdRequest delReq) throws TerminatorServiceException {
		DeleteUpdateCommand delCmd = new DeleteUpdateCommand();
		delCmd.id = delReq.id;
		delCmd.fromCommitted = true;
		delCmd.fromPending = true;
		
		try {
			this.solrCore.getUpdateHandler().delete(delCmd);
		} catch (IOException e) {
			throw new TerminatorServiceException("deleeteById.error",e);
		}
		return 0;
	}
	
	
	public int delete(DeleteByQueryRequest delReq) throws TerminatorServiceException {
		DeleteUpdateCommand delCmd = new DeleteUpdateCommand();
		delCmd.query = delReq.query;
		delCmd.fromCommitted = true;
		delCmd.fromPending = true;
		
		try {
			this.solrCore.getUpdateHandler().delete(delCmd);
		} catch (IOException e) {
			throw new TerminatorServiceException("deleeteById.error",e);
		}
		return 0;
	}

	@Override
	public int madd(List<AddDocumentRequest> addReqs) throws TerminatorServiceException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int mdelete(List<DeleteByIdRequest> delReqs) throws TerminatorServiceException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int mupdate(List<UpdateDocumentRequest> updateReqs) throws TerminatorServiceException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int mdeleteByQuery(List<DeleteByQueryRequest> delReqs) throws TerminatorServiceException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Address ping() throws TerminatorServiceException {
		// TODO Auto-generated method stub
		return null;
		
	}


}
