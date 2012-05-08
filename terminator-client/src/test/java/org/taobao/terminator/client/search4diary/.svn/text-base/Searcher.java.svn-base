package org.taobao.terminator.client.search4diary;

import com.taobao.terminator.common.protocol.TerminatorQueryRequest;

public class Searcher extends Base{
	public static void main(String[] args) throws Exception {
		TerminatorQueryRequest q = new TerminatorQueryRequest();
		q.setQuery("df");
		q.setHighlight(true);
		q.addHighlightField("content");
		q.setHighlightSimplePre("<emem>");
		q.setHighlightSimplePost("</emem>");
		System.out.println(bean.query(q));
		
	}
}
