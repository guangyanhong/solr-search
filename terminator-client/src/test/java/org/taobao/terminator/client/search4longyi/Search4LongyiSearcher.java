package org.taobao.terminator.client.search4longyi;

import com.taobao.terminator.client.TerminatorBean;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.terminator.common.protocol.TerminatorQueryRequest;

public class Search4LongyiSearcher {
	private static TerminatorBean bean;
	
	static{
		ApplicationContext context = new ClassPathXmlApplicationContext("termiantor-client-search4longyi.xml");
		bean = (TerminatorBean)context.getBean("terminator");
	}
	
	public static void main(String[] args) throws Exception {
		TerminatorQueryRequest query = new TerminatorQueryRequest();
		query.setQuery("title1:ĸӤ");
		query.setStart(0);
		query.setRows(50);
		QueryResponse response = bean.query(query);
		System.out.println(response);
		Thread.sleep(20000);
	}
}
