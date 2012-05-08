package org.taobao.terminator.client.search4diary;

import com.taobao.terminator.client.TerminatorBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Base {
	protected static TerminatorBean bean;
	
	static{
		ApplicationContext context = new ClassPathXmlApplicationContext("termiantor-client-search4diary.xml");
		bean = (TerminatorBean)context.getBean("terminator");
	}
	
}