package com.taobao.terminator.common.protocol;

import java.util.List;

import com.taobao.terminator.common.TerminatorServiceException;

/**
 * 接收实时请求的Service
 * 
 * @author yusen
 */
public interface RealTimeService {
	public int madd(List<AddDocumentRequest> addReqs) throws TerminatorServiceException;
	
	public int mupdate(List<UpdateDocumentRequest> updateReqs) throws TerminatorServiceException;
	
	public int mdelete(List<DeleteByIdRequest> delReqs) throws TerminatorServiceException;
	
	public int mdeleteByQuery(List<DeleteByQueryRequest> delReqs) throws TerminatorServiceException;
	
	public int add(AddDocumentRequest addReq) throws TerminatorServiceException;
	
	public int update(UpdateDocumentRequest updateReq) throws TerminatorServiceException;

	public int delete(DeleteByIdRequest delReq) throws TerminatorServiceException;
	
	public int delete(DeleteByQueryRequest delReq) throws TerminatorServiceException;

	public Address ping() throws TerminatorServiceException;
	
	public static class Utils {
		public static String genHsfVersion(String coreName) {
			return new StringBuilder().append("REAL-TIME-SERVICE").append("-").append(coreName).toString();
		}
	}
}
