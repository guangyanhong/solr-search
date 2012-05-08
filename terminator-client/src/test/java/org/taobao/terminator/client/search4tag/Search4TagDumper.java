package org.taobao.terminator.client.search4tag;

import com.taobao.terminator.client.TerminatorBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Search4TagDumper {
	private static TerminatorBean bean;
	
	static{
		ApplicationContext context = new ClassPathXmlApplicationContext("termiantor-client-search4tag.xml");
		bean = (TerminatorBean)context.getBean("terminator");
	}
	
	public static void main(String[] args) {
		bean.triggerFullDumpJob();
//		bean.triggerIncrDumpJob();
//		bean.triggerFullDumpJob();
	}
}
