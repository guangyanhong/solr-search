package com.taobao.terminator.core.index.consumer;

import java.io.File;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.taobao.terminator.common.constant.IndexEnum;

public class FullIndexConsumer extends IndexConsumer {
	protected static final int BUFFER_SIZE = 1024 * 1024 * 10;
	protected ByteBuffer buffer;
	
	public FullIndexConsumer() {
		super();
	}

	public FullIndexConsumer(File indexDir, int jobnum) {
		super(indexDir, jobnum);
	}

	public void init(){
		super.init();
		logger.warn("[FULL] ==> 初始化全量消费者，缓存大小为：" + BUFFER_SIZE);
		this.buffer = ByteBuffer.allocate(BUFFER_SIZE);
	}
	
	public void start(){
		logger.warn("[FULL] ==> 开始一次全量任务请求，接下来会传送N批数据过来。。。");
		this.startTime = new Date();
	}

	public void finish() {
		logger.debug("[FULL] ==> 等待所有写全量索引文件的任务结束，当前还有<" + this.threadPool.getActiveCount() + ">个任务正在执行。。。");
		Thread finishThread = new Thread(new FinishTask());
		finishThread.setName("fullIndexConsumerFinishThread");
		finishThread.start();
		try {
			finishThread.join();
		} catch (InterruptedException e) {
			logger.error("",e);
			Thread.currentThread().interrupt();
		}
	}
	
	public void consum(byte[] data) {	
		if(data == null || data.length == 0) return;
		synchronized(this.buffer){
			if(this.buffer.remaining() >= data.length){
				this.buffer.put(data);
			} else{
				logger.warn("[FULL] ==>Buffer中已经有足够的数据，要写入文件,数据大小:" + data.length);
				this.buffer.flip();
				byte[] tmp = new byte[this.buffer.limit()];
				this.buffer.get(tmp);
				this.buffer.clear();
				this.buffer.put(data);
				long start = System.currentTimeMillis();
				new WriteXmlFileJob(tmp).run(); //FIXME TODO 改用线程池实现，暂时先这样吧
				logger.warn("写文件耗时 " + (System.currentTimeMillis() - start) + "ms 文件大小 :" + tmp.length );
			}
		}
	}

	@Override
	protected synchronized String getFilePath(String suffix) throws Exception {
		Thread.sleep(50);
		SimpleDateFormat formater = new SimpleDateFormat(IndexEnum.DATE_TIME_PATTERN.getValue());
		return this.indexDir.getAbsolutePath() + File.separator + formater.format(new Date()) + suffix;
	}
	
	/**
	 * 将Buffer中存余的数据flush到磁盘 
	 */
	protected class FinishTask implements Runnable{
		public void run() {
			if(buffer.position() > 0){
				logger.warn("[FULL] ==> 结束增量的扫尾工作，把Buffer中的数据Flush到磁盘中。");
				buffer.flip();
				byte[] tmp = new byte[buffer.limit()];
				buffer.get(tmp);
				buffer.clear();
				new WriteXmlFileJob(tmp).run();//FIXME TODO 改用线程池实现，暂时先这样吧
			}

			deletePendingFiles();
			endTime = new Date();
			logger.warn("[FULL] ==> 结束此次全量，开始时间为:" + startTime + "结束时间为:" + endTime + " 共耗时 :[" + (endTime.getTime() - startTime.getTime())/1000 + "] 秒");
		}
	}
}
