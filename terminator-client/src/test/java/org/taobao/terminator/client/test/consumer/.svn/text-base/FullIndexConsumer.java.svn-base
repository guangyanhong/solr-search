package org.taobao.terminator.client.test.consumer;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.taobao.terminator.common.constant.IndexEnum;
import org.taobao.terminator.client.test.consumer.IndexContext;

public class FullIndexConsumer extends AbstractIndexConsumer {
	
	public FullIndexConsumer(File indexDir) {
		super(indexDir);
	}
	
	public FullIndexConsumer(String indexDir) {
		super(indexDir);
	}
	
	public FullIndexConsumer(String indexDir, int numOfJobs){
		super(indexDir, numOfJobs);
	}
	
	public FullIndexConsumer(File indexDir, int numOfJobs){
		super(indexDir, numOfJobs);
	}
	
	protected void doFinish(){
		logger.warn("本次全量索引构建完成，没有被成功写入的索引文件有<" + this.pendingFiles.size() + ">个。pendings:" + this.pendingFiles);
		//删除没有被成功写入的索引文件
		for(Map.Entry<String, File> entry : this.pendingFiles.entrySet()){
			if(entry.getValue().delete()){
				logger.warn("删除错误的索引文件成功，file:" + entry.getValue().getName());
			} else{
				logger.warn("删除错误的索引文件失败，file:" + entry.getValue().getName());
			}
		}
		this.pendingFiles.clear();
		this.endTime = new Date();
		logger.debug("full index dump finished...索引xml数据传送的开始时间为:" + this.startTime + "结束时间为:" + this.endTime);
		IndexContext.isDataTransmitFinish.set(true);
	}
	
	public boolean start(){
		this.threadPool = new ThreadPoolExecutor(this.numOfJobs, this.numOfJobs, 
				Long.MAX_VALUE, TimeUnit.NANOSECONDS, new LinkedBlockingQueue<Runnable>());
		IndexContext.isDataTransmitFinish.set(false);		
		this.startTime = new Date();
		return true;
	}
	
	private class FinishThread implements Runnable{
		public void run() {
			//如果缓存中有数据，那么将缓存中的数据写入文件
			if(FullIndexConsumer.this.buffer.position() > 0){
				FullIndexConsumer.this.buffer.flip();
				byte[] tmp = new byte[FullIndexConsumer.this.buffer.limit()];
				FullIndexConsumer.this.buffer.get(tmp);
				FullIndexConsumer.this.buffer.clear();
				FullIndexConsumer.this.threadPool.execute(new FullIndexConsumer.WriterIndexJob(tmp,null));
			}
			
			FullIndexConsumer.this.threadPool.shutdown();
			try {
				while(!FullIndexConsumer.this.threadPool.awaitTermination(1000, TimeUnit.NANOSECONDS)){
					logger.debug("当前还有<" + FullIndexConsumer.this.threadPool.getActiveCount() + ">个写全量索引文件的任务正在运行。。。");
				}
			} catch (InterruptedException e) {
				logger.error("等待所有写全量索引文件的任务完成时被中断。。。", e);
				throw new RuntimeException(Thread.currentThread().toString() + " is interrupted.");
			}
			FullIndexConsumer.this.doFinish();
		}
		
	}
	public boolean finish() {
			logger.debug("等待所有写全量索引文件的任务结束，当前还有<" + this.threadPool.getActiveCount() + ">个任务正在执行。。。");
			Thread finishThread = new Thread(new FinishThread());
			finishThread.setName("fullIndexConsumerFinishThread");
			finishThread.start();
			
			return true;
	}

	@Override
	protected synchronized String getFilePath(String suffix) throws Exception {
		Thread.sleep(50);
		SimpleDateFormat formater = new SimpleDateFormat(IndexEnum.DATE_TIME_PATTERN.getValue());
		return this.indexDir.getAbsolutePath() + File.separator + formater.format(new Date()) + suffix;
	}
}
