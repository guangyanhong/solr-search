package org.taobao.terminator.client.search4comment;


public class ThreadTest {
	public static void main(String[] args) throws InterruptedException {
		Thread t1 = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					System.out.println("fda");
				}
			}
		});
		t1.start();
		
		t1.interrupt();
	}
}
