package com.taobao.terminator.core.realtime;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.TerminatorServiceException;
import com.taobao.terminator.common.protocol.AddDocumentRequest;
import com.taobao.terminator.common.protocol.Address;
import com.taobao.terminator.common.protocol.DeleteByIdRequest;
import com.taobao.terminator.common.protocol.DeleteByQueryRequest;
import com.taobao.terminator.common.protocol.RealTimeService;
import com.taobao.terminator.common.protocol.UpdateDocumentRequest;
import com.taobao.terminator.core.realtime.commitlog2.CommitLogAccessor;

/**
 * 默认的RealTimeService,直接写BlockingQueue然后立马返回，WriteCommitLogJob线程会从队列中读取写入CommitLog
 * 
 * @author yusen
 */
public class DefaultRealTimeService implements RealTimeService{
	protected static Log log = LogFactory.getLog(RealTimeService.class);
	private CommitLogAccessor commitLogAccessor;
	
	public DefaultRealTimeService(CommitLogAccessor commitLogAccessor) {
		this.commitLogAccessor = commitLogAccessor;
	}
	
	public int madd(List<AddDocumentRequest> addReqs) throws TerminatorServiceException {
		for(AddDocumentRequest obj : addReqs) {
			this.writeToCommitLog(obj);
		}
		
		return addReqs.size();
	}
	
	public int mupdate(List<UpdateDocumentRequest> updateReqs) throws TerminatorServiceException {
		for(UpdateDocumentRequest obj : updateReqs) {
			this.writeToCommitLog(obj);
		}
		return updateReqs.size();
	}
	
	public int mdelete(List<DeleteByIdRequest> delReqs) throws TerminatorServiceException {
		for(DeleteByIdRequest obj : delReqs) {
			this.writeToCommitLog(obj);
		}
		
		return delReqs.size();
	}
	
	public int mdeleteByQuery(List<DeleteByQueryRequest> delReqs) throws TerminatorServiceException {
		for(DeleteByQueryRequest obj : delReqs) {
			this.writeToCommitLog(obj);
		}
		
		return delReqs.size();
	}
	
	public int add(AddDocumentRequest addReq) throws TerminatorServiceException {
		this.writeToCommitLog(addReq);
		return 1;
	}
	
	public int update(UpdateDocumentRequest updateReq) throws TerminatorServiceException {
		this.writeToCommitLog(updateReq);
		return 1;
	}

	public int delete(DeleteByIdRequest delReq) throws TerminatorServiceException {
		this.writeToCommitLog(delReq);
		return 1;
	}
	
	public int delete(DeleteByQueryRequest delReq) throws TerminatorServiceException {
		this.writeToCommitLog(delReq);
		return 1;
	}

	private void writeToCommitLog(Object obj) throws TerminatorServiceException {
		try {
			commitLogAccessor.write(obj);
		} catch (Exception e) {
			throw new TerminatorServiceException("", e);
		}
	}

	@Override
	public Address ping() throws TerminatorServiceException {
		String ip = TerminatorCommonUtils.getLocalHostIP();
		int port = 1230;
		return new Address(ip, port);
	}
}
