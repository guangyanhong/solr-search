package com.taobao.terminator.core.realtime.commitlog;

import java.io.File;
import java.io.IOException;

import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import com.taobao.terminator.common.protocol.AddDocumentRequest;


public class CommitLogTest {
	private static final String base_dir = "C:\\commitLog2";
	private static CommitLog commitLog = null;

	static {
		try {
			commitLog = new CommitLog(new File(base_dir),1024 * 1024 * 100);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testWrite() throws IOException, InterruptedException {
		long start = System.currentTimeMillis();
		int count = 0;
		for (int i = 0; i < 2000000; i++) {
			AddDocumentRequest addReq = new AddDocumentRequest();
			SolrInputDocument solrDoc = new SolrInputDocument();
			String id = "2002";
			solrDoc.addField("id", "" + id);
			solrDoc.addField("title", "titlefdafda22" + id);
			solrDoc.addField("content1", "content" + id);
			solrDoc.addField("content2", "contentcontentcontentcontentcontentcontent" + id);
			solrDoc.addField("content3", "contentcontentcontentcontentcontent" + id);
			solrDoc.addField("content4", "contcontentent" + id);
			addReq.solrDoc = solrDoc;
			commitLog.writeObject(addReq);
			count = count + 1;
			if((System.currentTimeMillis() - start) >=1 * 1000) {
				System.out.println("Write count per second ==> " + count);
				count = 0;
				start = System.currentTimeMillis();
			}
		}
	}
	
	@Test
	public void testRead() throws Exception {
		long start = System.currentTimeMillis();
		int count = 0;
		while(commitLog.readObject()!= null) {
			count = count + 1;
			if((System.currentTimeMillis() - start) >=1 * 1000) {
				System.out.println("Read count per second ==> " + count);
				count = 0;
				start = System.currentTimeMillis();
			}
		}
	}
}
