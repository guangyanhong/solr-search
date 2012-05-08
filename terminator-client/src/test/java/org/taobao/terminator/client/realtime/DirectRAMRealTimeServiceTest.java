//package org.taobao.terminator.client.realtime;
//
//import org.apache.solr.client.solrj.SolrQuery.ORDER;
//import org.apache.solr.common.SolrInputDocument;
//import org.junit.Test;
//
//import com.taobao.hsfunit.app.spring.util.HSFSpringConsumerBean;
//import com.taobao.terminator.common.protocol.AddDocumentRequest;
//import com.taobao.terminator.common.protocol.DeleteByIdRequest;
//import com.taobao.terminator.common.protocol.RealTimeService;
//import com.taobao.terminator.common.protocol.SearchService;
//import com.taobao.terminator.common.protocol.TerminatorQueryRequest;
//
///**
// *  id,title,content,gmt_modified
// *
// * @author yusen
// */
//public class DirectRAMRealTimeServiceTest {
//	protected static RealTimeService service;
//	protected static SearchService   search;
//
//	static {
//		HSFSpringConsumerBean hsfConsumer = new HSFSpringConsumerBean();
//		hsfConsumer.setInterfaceName(RealTimeService.class.getName());
//		hsfConsumer.setVersion("test");
////		hsfConsumer.setTarget("10.232.12.85:12200?CLIENTRETRYCONNECTIONTIMES=3&CLIENTRETRYCONNECTIONTIMEOUT=1000&_SERIALIZETYPE=java&_IDLETIMEOUT=600&_TIMEOUT=3000");
//		try {
//			hsfConsumer.init();
//			service = (RealTimeService)(hsfConsumer.getObject());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		HSFSpringConsumerBean hsfConsumer2 = new HSFSpringConsumerBean();
//		hsfConsumer2.setInterfaceName(SearchService.class.getName());
//		hsfConsumer2.setVersion("test");
////		hsfConsumer2.setTarget("10.232.12.85:12200?CLIENTRETRYCONNECTIONTIMES=3&CLIENTRETRYCONNECTIONTIMEOUT=1000&_SERIALIZETYPE=java&_IDLETIMEOUT=600&_TIMEOUT=3000");
//		try {
//			hsfConsumer2.init();
//			search = (SearchService)(hsfConsumer2.getObject());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//
///*	public void testRealTimeAddAndQueryIt() throws Exception{
//		SolrInputDocument solrDoc = new SolrInputDocument();
//		solrDoc.addField("id", "2002");
//		solrDoc.addField("title", "title2002");
//		solrDoc.addField("content", "content2002");
//		service.add(solrDoc);
//
//
//		TerminatorQueryRequest query = new TerminatorQueryRequest();
//		query.setFields(new String[]{"id","title","content"});
//		query.setSortField("id", ORDER.desc);
//		query.setQuery("*:*");
//		System.out.println(service.query(query));
//
//	}*/
//
//	@Test
//	public void testAddSome()throws Exception{
//		for(int i =3501; i<= 4000;i++) {
//			AddDocumentRequest addReq = new AddDocumentRequest();
//			SolrInputDocument solrDoc = new SolrInputDocument();
//			String id = "" + i;
//			solrDoc.addField("id", "" + id);
//			solrDoc.addField("title", "title22" + id);
//			solrDoc.addField("content", "content" + id);
//			addReq.solrDoc = solrDoc;
//			service.add(addReq);
//			Thread.sleep(100);
//		}
//	}
//
//	@Test
//	public void testAddOne() throws Exception {
//		AddDocumentRequest addReq = new AddDocumentRequest();
//		SolrInputDocument solrDoc = new SolrInputDocument();
//		String id = "2001";
//		solrDoc.addField("id", "" + id);
//		solrDoc.addField("title", "titlefdafda22" + id);
//		solrDoc.addField("content", "content" + id);
//		addReq.solrDoc = solrDoc;
//		service.add(addReq);
//	}
//
//	@Test
//	public void testDelete() throws Exception {
//		for(int i = 1000; i<= 2000;i++) {
//			DeleteByIdRequest delReq = new DeleteByIdRequest();
//			delReq.id ="" + i;
//			service.delete(delReq);
//		}
//	}
//
//	@Test
//	public void testDeleteOne() throws Exception {
//		DeleteByIdRequest delReq = new DeleteByIdRequest();
//		delReq.id = "20010";
//		service.delete(delReq);
//	}
//
//	@Test
//	public void testQuery() throws Exception{
//		TerminatorQueryRequest query = new TerminatorQueryRequest();
//		query.setFields(new String[]{"id","title","content"});
//		query.setSortField("id", ORDER.desc);
//		query.setQuery("*:*");
//		System.out.println(search.query(query));
//	}
//}