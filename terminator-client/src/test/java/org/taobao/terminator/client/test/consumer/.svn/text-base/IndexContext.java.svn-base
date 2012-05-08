package org.taobao.terminator.client.test.consumer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 这个类中的常量用来记录索引的状态
 * @author tianxiao
 */
public class IndexContext {
	/**
	 * 用来记录当前是否正在进行全量索引
	 */
	public static AtomicBoolean isFullIndexing = new AtomicBoolean(false);
	
	/**
	 * 用来记录当前是否正在进行增量索引
	 */
	public static AtomicBoolean isIndrIndexing = new AtomicBoolean(false);
	
	/**
	 * 用来记录全量索引时，所有的数据是否都已经传送完成。
	 * 默认为true，表示没有增量数据传送，或者已经传送完成。
	 * 当开始增量之后，这个变量会被设置成false，表示正在进行增量数据的传送。
	 */
	public static AtomicBoolean isDataTransmitFinish = new AtomicBoolean(true);
}
