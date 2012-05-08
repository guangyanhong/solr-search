package org.taobao.terminator.client.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.taobao.terminator.client.TerminatorBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.terminator.client.router.AbstractGroupRouter;
import com.taobao.terminator.common.protocol.TerminatorQueryRequest;

public class TerminatorBeanTest {
	private static TerminatorBean bean;
	private static boolean shardQuery = true;
	
	static{
		ApplicationContext context = new ClassPathXmlApplicationContext("dataprovider-example.xml");
		bean = (TerminatorBean)context.getBean("terminator");
	}
	
	public static void main(String[] args) throws Exception {
//		multiFullJob();
//		multiIncrJob();
//		testQuery();
		while(true){
			Thread.sleep(100);
		}
	}
	
	public static void multiFullJob(){
		ExecutorService executors = Executors.newFixedThreadPool(3);
		for(int i = 0; i<1 ;i++){
			Runnable task = new Runnable() {
				@Override
				public void run() {
					bean.triggerFullDumpJob();
				}
			};
			executors.execute(task);
		}
	}
	
	public static void multiIncrJob(){
		ExecutorService executors = Executors.newFixedThreadPool(3);
		for(int i = 0; i<1 ;i++){
			Runnable task = new Runnable() {
				@Override
				public void run() {
					bean.triggerIncrDumpJob();
				}
			};
			executors.execute(task);
		}
	}
	
	public static void testQuery() throws Exception {
		System.out.println("");
		TerminatorQueryRequest query = new TerminatorQueryRequest();
		query.setQuery("memo:jpg");
		Map<String,String> map = new HashMap<String,String>();
		map.put("userId", "1");
		if(shardQuery){
			query.addRouteValue(map);
		}
		System.out.println(bean.query(query));
		Thread.sleep(20000);
		
		TerminatorQueryRequest query1 = new TerminatorQueryRequest();
		query1.setQuery("memo:jpg");
		Map<String,String> map1 = new HashMap<String,String>();
		map.put("userId", "1");
		if(shardQuery){
			query1.addRouteValue(map1);
		}
		System.out.println(bean.query(query1));
	}
	
	public static class MyRouter extends AbstractGroupRouter{
		@Override
		public String getGroupName(Map<String, String> rowData) {
			String obj = rowData.get("userId");
			Long uid = Long.valueOf(obj);
			int groupNum = serviceConfig.getGroupNum();
			return String.valueOf(uid % groupNum);
		}
	}
}