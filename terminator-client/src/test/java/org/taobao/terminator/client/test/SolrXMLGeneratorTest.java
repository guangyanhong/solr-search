package org.taobao.terminator.client.test;

import org.junit.Test;

import com.taobao.terminator.client.index.data.xml.SolrXmlDocGenerator;

public class SolrXMLGeneratorTest {
	private static SolrXmlDocGenerator xmlGenerator = new SolrXmlDocGenerator();
	
	@Test
	public void test() throws Exception{
		String result = xmlGenerator.genSolrDeleteXMLByQuery("111111");
		
		System.out.println(result);
	}
}
