package org.taobao.terminator.client.search4tstar;

import com.taobao.terminator.client.ThinTerminatorBean;
import org.apache.solr.client.solrj.response.QueryResponse;

import com.taobao.terminator.common.TerminatorServiceException;
import com.taobao.terminator.common.protocol.TerminatorQueryRequest;

public class ClientTest {
	public static void main(String[] args) throws TerminatorServiceException {
		ThinTerminatorBean bean = new ThinTerminatorBean();
		bean.setZkAddress("192.168.211.29:2181");
		bean.setZkTimeout(3000000);
		bean.setServiceName("search4tstar");
		
		bean.init();
		
		TerminatorQueryRequest query = new TerminatorQueryRequest();
		query.setQuery("*:*");
		query.setStart(0);
		query.setRows(50);
		QueryResponse response = bean.query(query);
		System.out.println(response);
	}
}