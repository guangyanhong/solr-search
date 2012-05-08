package org.taobao.terminator.client.search4tag;

import com.taobao.terminator.client.TerminatorBean;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.terminator.common.protocol.TerminatorQueryRequest;

public class Search4TagSearcher {
	private static TerminatorBean bean;
	private static boolean shardQuery = true;
	
	static{
		ApplicationContext context = new ClassPathXmlApplicationContext("termiantor-client-search4tag.xml");
		bean = (TerminatorBean)context.getBean("terminator");
	}
	
	public static void main(String[] args) throws Exception {
		TerminatorQueryRequest query = new TerminatorQueryRequest();
		query.setQuery("tags:IBM^3.67");
		query.setStart(0);
		query.setRows(50);
		QueryResponse response = bean.query(query);
		System.out.println(response);
		Thread.sleep(20000);
	}
}
