package org.taobao.terminator.client.search4zhaopin;

import com.taobao.terminator.client.TerminatorBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Base {
	protected static TerminatorBean resumeSearchTerminator;
	protected static TerminatorBean jobSearchTerminator;
	
	static{
		ApplicationContext context = new ClassPathXmlApplicationContext("terminator-client-search4zhaopin.xml");
//		resumeSearchTerminator = (TerminatorBean4Test)context.getBean("resumeSearchTerminator");
		jobSearchTerminator = (TerminatorBean)context.getBean("jobSearchTerminator");
	}
	
}
