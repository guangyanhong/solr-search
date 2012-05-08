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
		logger.warn("����ȫ������������ɣ�û�б��ɹ�д��������ļ���<" + this.pendingFiles.size() + ">����pendings:" + this.pendingFiles);
		//ɾ��û�б��ɹ�д��������ļ�
		for(Map.Entry<String, File> entry : this.pendingFiles.entrySet()){
			if(entry.getValue().delete()){
				logger.warn("ɾ������������ļ��ɹ���file:" + entry.getValue().getName());
			} else{
				logger.warn("ɾ������������ļ�ʧ�ܣ�file:" + entry.getValue().getName());
			}
		}
		this.pendingFiles.clear();
		this.endTime = new Date();
		logger.debug("full index dump finished...����xml���ݴ��͵Ŀ�ʼʱ��Ϊ:" + this.startTime + "����ʱ��Ϊ:" + this.endTime);
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
			//��������������ݣ���ô�������е�����д���ļ�
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
					logger.debug("��ǰ����<" + FullIndexConsumer.this.threadPool.getActiveCount() + ">��дȫ�������ļ��������������С�����");
				}
			} catch (InterruptedException e) {
				logger.error("�ȴ�����дȫ�������ļ����������ʱ���жϡ�����", e);
				throw new RuntimeException(Thread.currentThread().toString() + " is interrupted.");
			}
			FullIndexConsumer.this.doFinish();
		}
		
	}
	public boolean finish() {
			logger.debug("�ȴ�����дȫ�������ļ��������������ǰ����<" + this.threadPool.getActiveCount() + ">����������ִ�С�����");
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
