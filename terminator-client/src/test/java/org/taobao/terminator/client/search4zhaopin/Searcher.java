package org.taobao.terminator.client.search4zhaopin;

import com.taobao.terminator.common.protocol.TerminatorQueryRequest;

public class Searcher extends Base{
	public static void main(String[] args) throws Exception {
		TerminatorQueryRequest q = new TerminatorQueryRequest();
		q.setQuery("id:0-20264");
		System.out.println(resumeSearchTerminator.query(q));
	}
}
