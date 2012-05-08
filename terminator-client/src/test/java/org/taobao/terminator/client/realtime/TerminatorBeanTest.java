package org.taobao.terminator.client.realtime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import com.taobao.terminator.client.realtime.TerminatorBean;
import com.taobao.terminator.common.TerminatorServiceException;
import com.taobao.terminator.common.protocol.AddDocumentRequest;
import com.taobao.terminator.common.protocol.DeleteByIdRequest;
import com.taobao.terminator.common.protocol.TerminatorQueryRequest;
import com.taobao.terminator.common.protocol.UpdateDocumentRequest;

public class TerminatorBeanTest {
	private  static TerminatorBean bean;
	
	static {
		bean = new TerminatorBean();
		bean.setZkAddress("10.232.15.46:2181");
		bean.setServiceName("search4ecrmtest");
		bean.setZkTimeout(3000000);
		bean.setServiceConfigChangeAware(true);
		bean.init();
	}
	
	@Test
	public void teseQuery() throws Exception {
		TerminatorQueryRequest query = new TerminatorQueryRequest();
		query.setFields(new String[]{"id","nick"});
		query.setSortField("id", ORDER.desc);
		query.setQuery("id:789123");
		System.out.println(bean.query(query));
	}
	
	@Test
	public void testAddOne() throws Exception {
		AddDocumentRequest addReq = new AddDocumentRequest();
		SolrInputDocument solrDoc = new SolrInputDocument();
		String id = "40001";
		solrDoc.addField("id", id);
		solrDoc.addField("s_id", id);
		solrDoc.addField("nick", "yusen" + id);
		addReq.solrDoc = solrDoc;
		System.out.println(bean.add(addReq));
	}
	
	@Test
	public void testUpdate() throws Exception {
		UpdateDocumentRequest updateReq = new UpdateDocumentRequest();
		SolrInputDocument solrDoc = new SolrInputDocument();
		String id = "1";
		solrDoc.addField("id", id);
		solrDoc.addField("s_id", id);
		solrDoc.addField("nick", "yusen-update2ffdasda22" + id);
		updateReq.solrDoc = solrDoc;
		System.out.println(bean.update(updateReq));
	}
	
	@Test
	public void testDeleteOne() throws Exception {
		DeleteByIdRequest del = new DeleteByIdRequest();
		del.id = "52000";
		System.out.println(bean.delete(del));
	}
	
	@Test
	public void testAddSome() throws Exception {
		final int start = 100000;
		final int count = 100000;
		for (int i = start; i < start + count; i++) {
			AddDocumentRequest addReq = new AddDocumentRequest();
			SolrInputDocument solrDoc = new SolrInputDocument();

			solrDoc.addField("id", "" + i);
			solrDoc.addField("s_id", "" + i);
			solrDoc.addField("nick", "yusen" + i);
			addReq.solrDoc = solrDoc;

			Thread.sleep(500);
			bean.add(addReq);
			System.out.println(i - start);
		}
	}
	
	public static void main(String[] args) throws Exception {
		long s = System.currentTimeMillis();
		ThreadPoolExecutor executors = (ThreadPoolExecutor)Executors.newFixedThreadPool(5);
		final int count = 40000;
		final int start = 3000000;
		for(int i = start; i < start + count;i++) {
			executors.execute(new RTJob(i));
		}
		
		while(executors.getActiveCount() != 0) {
			Thread.sleep(2000);
		}
		
		System.out.println("Time : " + count / ((System.currentTimeMillis() - s) /1000));
	}
	
	public static class RTJob implements Runnable {
		private int id;
		
		public RTJob(int id) {
			this.id = id;
		}
		
		public void run() {
			AddDocumentRequest addReq = new AddDocumentRequest();
			SolrInputDocument solrDoc = new SolrInputDocument();

			solrDoc.addField("id", "" + id);
			solrDoc.addField("s_id", "" + id);
			solrDoc.addField("nick", "yusen" + id);
			addReq.solrDoc = solrDoc;
			
			try {
				bean.add(addReq);
			} catch (TerminatorServiceException e) {
				
			}
		}
	}
	
	@Test
	public void testDeleteSome() throws Exception {
		int start = 2000000;
//		List<DeleteByIdRequest> list = new ArrayList<DeleteByIdRequest>();
		for(int i = start; i<start + 400000;i++) {
			DeleteByIdRequest delReq = new DeleteByIdRequest();
			delReq.id = i+"";
//			list.add(delReq);
			
			System.out.println(bean.delete(delReq));
		}
	}
	
	@Test
	public void testUpdateSome() throws Exception {
		int start = 100000;
		List<UpdateDocumentRequest> list = new ArrayList<UpdateDocumentRequest>();
		for(int i = start; i<start + 40000;i++) {
			UpdateDocumentRequest addReq = new UpdateDocumentRequest();
			SolrInputDocument solrDoc = new SolrInputDocument();
			
			solrDoc.addField("id", "" + i);
			solrDoc.addField("s_id", "" + i);
			solrDoc.addField("nick", "yusen" + i);
			addReq.solrDoc = solrDoc;
			
			list.add(addReq);
		}
		System.out.println(bean.mupdate(list));
	}
}
