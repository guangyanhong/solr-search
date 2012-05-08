package com.taobao.terminator.common.zk;

import org.apache.zookeeper.WatchedEvent;
import org.junit.Before;
import org.junit.Test;

public class TerminatorZkClientTest {
	
	TerminatorZkClient client = null;
	
	@Before
	public void setUp() throws Exception{
		client = new TerminatorZkClient("192.168.211.29:2181");
	}
	
	public void after() throws Exception{
	}
	
	@Test
	public void testCreatePath() throws Exception{
		client.createPath("/createPath/createPath");
	}
	
	@Test
	public void testRCreatePath() throws Exception{
		client.rcreatePath("/rcreatePath/rcreatePath");
	}
	
	@Test
	public void testSetData() throws Exception{
		client.setData("/setData/setData/", TerminatorZKUtils.toBytes("this is /setData/setData/"));
	}
	
	@Test
	public void testGetData()throws Exception{
		byte[] bs = client.getData("setData/setData");
		System.out.println(TerminatorZKUtils.toString(bs));
	}
	
	@Test
	public void testExist() throws Exception{
		boolean  b = client.exists("setData/");
		System.out.println(b);
	}
	
	@Test
	public void testRcreate() throws Exception{
		client.rcreate("/hello/hello", TerminatorZKUtils.toBytes("this is /hello/hello"));
	}
	
	@Test
	public void testDelete() throws Exception{
		client.delete("/hello/hello");
	}
	
	@Test
	public void testRdelete() throws Exception{
		client.rdelete("/terminator/");
	}
	
	private static  TerminatorZkClient zkclient;
	static{
		try {
			zkclient = new TerminatorZkClient("192.168.211.29:2181");
		} catch (TerminatorZKException e) {
			e.printStackTrace();
		}
	}
	
	public static void exist() throws TerminatorZKException{
		zkclient.exists("hello/hello",new TerminatorWatcher(){
			@Override
			public void handle(WatchedEvent event, TerminatorZkClient zkClient) {
				System.out.println(event.getType());
			}
		});
	}
	
	
	public static void getChildren() throws TerminatorZKException{
		zkclient.getChildren("hello/hello",new TerminatorWatcher(){
			@Override
			public void handle(WatchedEvent event, TerminatorZkClient zkClient) {
				System.out.println(event.getType());
			}
		});
	}
	
	public static void getData() throws TerminatorZKException{
		zkclient.getData("hello/hello",new TerminatorWatcher(){
			@Override
			public void handle(WatchedEvent event, TerminatorZkClient zkClient) {
				System.out.println("DataChanged");
			}
		});
	}
	
	public static void main(String[] args)  {
		try {
			TerminatorZkClient zkClient = new TerminatorZkClient("192.168.211.29:2181",3000,new OnReconnect() {
				@Override
				public void onReconnect(TerminatorZkClient zkClient) throws Exception {
					System.out.println("This is OnReconnect Listener");
				}
				
			},false);
		} catch (TerminatorZKException e) {
			e.printStackTrace();
		}
		
		while(true){
			
		}
	}
}
