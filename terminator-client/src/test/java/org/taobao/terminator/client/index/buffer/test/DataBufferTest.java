package org.taobao.terminator.client.index.buffer.test;

import org.junit.Before;
import org.junit.Test;

import com.taobao.terminator.client.index.buffer.DataBuffer;
import com.taobao.terminator.client.index.buffer.DataBuffer.CapacityInfo;

public class DataBufferTest {
	private DataBuffer dataBuffer = null;
	
	@Before
	public void setUp(){
		CapacityInfo c = new CapacityInfo();
		c.initcapacity = 2;
		c.capacityIncrement = 2;
		c.maxCapacity = 6;
		dataBuffer = new DataBuffer(c);
	}
	
	@Test
	public void testAppend(){
		byte[] data = new byte[]{1,2,3,4,5,6,7,8};
		dataBuffer.append(data);
		
		byte[] data2 = new byte[]{1,2,3,4,5,6,7,8};
		dataBuffer.append(data2);
	}
}
