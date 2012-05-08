package org.taobao.terminator.client.test;

import org.junit.Before;
import org.junit.Test;

import com.taobao.terminator.client.index.timer.ZKTimeManager;
import com.taobao.terminator.client.index.timer.TimerManager.StartAndEndTime;
import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.zk.TerminatorZkClient;

public class ZKTimeManagerTest {
	private ZKTimeManager manager = null;
	
	@Before
	public void setUp() throws Exception{
		TerminatorZkClient zkClient = new TerminatorZkClient("192.168.211.29:2181");
		ZKTimeManager.createInstance(zkClient,"search4album-0");
		manager = ZKTimeManager.getInstance("");
	}
	
	@Test
	public void testInit() throws Exception{
		StartAndEndTime times = manager.initTimes();
		System.out.println("此次增量的起始结束时间点分别为   ==> " + TerminatorCommonUtils.formatDate(times.startTime) + "     " + TerminatorCommonUtils.formatDate(times.endTime));
	}
	
	@Test
	public void testReset() throws Exception{
		manager.initTimes();
		StartAndEndTime times = manager.resetTimes();
		System.out.println("此次增量的起始结束时间点分别为   ==> " + TerminatorCommonUtils.formatDate(times.startTime) + "     " + TerminatorCommonUtils.formatDate(times.endTime));
		
	}
}
