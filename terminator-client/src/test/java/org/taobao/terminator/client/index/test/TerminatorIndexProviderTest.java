package org.taobao.terminator.client.index.test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.terminator.client.index.IndexProvider4Test;
import com.taobao.terminator.client.index.TerminatorIndexProvider;
import com.taobao.terminator.client.index.buffer.DataBuffer.CapacityInfo;
import com.taobao.terminator.client.index.data.DataProvider;
import com.taobao.terminator.client.index.data.procesor.DataProcessor;
import com.taobao.terminator.client.router.GroupRouter;
import com.taobao.terminator.common.config.ServiceConfig;
import com.taobao.terminator.common.constant.IndexType;

public class TerminatorIndexProviderTest {
	private DataProvider fullDataProvider = null;
	private DataProvider incrDataProvider = null;
	private TerminatorIndexProvider fullIndexProvider = null;
	private TerminatorIndexProvider incrIndexProvider = null;
	private DataProcessor dataProcessor = null;
	private GroupRouter   groupRouter = new GroupRouter() {
		@Override
		public String getGroupName(Map<String, String> rowData) {
			String id = rowData.get("id");
			return String.valueOf(Integer.valueOf(id) % 1);
		}
	};
	
	private CapacityInfo  fullCapacityInfo = new CapacityInfo();
	private CapacityInfo  incrCapacityInfo = new CapacityInfo();
	private String serviceName = "search4album";
	private ServiceConfig serviceConfig = new ServiceConfig(serviceName){
		public Set<String> getGroupNameSet(){
			Set<String> set = new HashSet<String>();
			set.add("0");
			return set;
			
		}
	};
	
	@Before
	public void setUp(){
		ApplicationContext context = new ClassPathXmlApplicationContext("dataprovider-example.xml");
		fullDataProvider = (DataProvider)context.getBean("fullDataProvider",DataProvider.class);
		incrDataProvider = (DataProvider)context.getBean("incrDataProvider",DataProvider.class);
		
		fullIndexProvider = new IndexProvider4Test(true);
		fullIndexProvider.setCapacityInfo(fullCapacityInfo);
		fullIndexProvider.setDataProcessor(dataProcessor);
		fullIndexProvider.setDataProvider(fullDataProvider);
		fullIndexProvider.setRouter(groupRouter);
		fullIndexProvider.setServiceConfig(serviceConfig);
		fullIndexProvider.setServiceName(serviceName);
		fullIndexProvider.setIndexType(IndexType.FULL);
		fullIndexProvider.afterPropertiesSet();
		
		incrIndexProvider = new IndexProvider4Test(true);
		incrIndexProvider.setCapacityInfo(incrCapacityInfo);
		incrIndexProvider.setDataProcessor(dataProcessor);
		incrIndexProvider.setDataProvider(incrDataProvider);
		incrIndexProvider.setRouter(groupRouter);
		incrIndexProvider.setServiceConfig(serviceConfig);
		incrIndexProvider.setServiceName(serviceName);
		incrIndexProvider.setIndexType(IndexType.INCREMENT);
		incrIndexProvider.afterPropertiesSet();
	}
	
	@Test
	public void testFullDump(){
		fullIndexProvider.dump();
	}
	
	@Test
	public void testIncrDump(){
		incrIndexProvider.dump();
	}
}
