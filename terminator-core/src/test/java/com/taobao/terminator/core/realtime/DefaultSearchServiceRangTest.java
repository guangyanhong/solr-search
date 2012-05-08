package com.taobao.terminator.core.realtime;

import junit.framework.TestCase;

import com.taobao.terminator.common.TerminatorServiceException;
import com.taobao.terminator.core.realtime.DefaultSearchService.Range;

public class DefaultSearchServiceRangTest extends TestCase {
	
	public void testXX() throws TerminatorServiceException {
		Range r = DefaultSearchService.Range.parse("test:[10 TO 20]");
		System.out.println(r);
	}

}
