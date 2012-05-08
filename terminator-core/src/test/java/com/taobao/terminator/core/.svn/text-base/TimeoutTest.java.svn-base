package com.taobao.terminator.core;

import org.junit.Test;

import com.taobao.terminator.core.util.TimeoutThread;

public class TimeoutTest {
	@Test
	public void test() throws InterruptedException{
		TestTimeoutHandler handler = new TestTimeoutHandler();
		
		TimeoutThread thread = new TimeoutThread(2000, handler, "handlerTimeout");
		thread.setName("TimeoutThread");
		thread.start();
		
		thread.sleepAgain();
		thread.interrupt();
		Thread.sleep(1000);
		
		thread.sleepAgain();
		thread.interrupt();
		Thread.sleep(1000);
		
		thread.sleepAgain();
		thread.interrupt();
		Thread.sleep(1000);
		
		/*thread.stopRunning();
		thread.interrupt();*/
		
		while(true){
			
		}
	}
}
