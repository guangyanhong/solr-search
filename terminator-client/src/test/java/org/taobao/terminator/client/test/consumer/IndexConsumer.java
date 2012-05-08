package org.taobao.terminator.client.test.consumer;

/**
 * 用来处理从客户端传过来的index数据，然后对这写数据进行处理
 * @author tianxiao
 *
 */
public interface IndexConsumer {
	public void consum(byte[] data,String filePath);
	public boolean start();
	public boolean finish();
}
