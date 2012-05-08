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
		logger.warn("[FULL] ==> ��ʼ��ȫ�������ߣ������СΪ��" + BUFFER_SIZE);
		this.buffer = ByteBuffer.allocate(BUFFER_SIZE);
	}
	
	public void start(){
		logger.warn("[FULL] ==> ��ʼһ��ȫ���������󣬽������ᴫ��N�����ݹ���������");
		this.startTime = new Date();
	}

	public void finish() {
		logger.debug("[FULL] ==> �ȴ�����дȫ�������ļ��������������ǰ����<" + this.threadPool.getActiveCount() + ">����������ִ�С�����");
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
				logger.warn("[FULL] ==>Buffer���Ѿ����㹻�����ݣ�Ҫд���ļ�,���ݴ�С:" + data.length);
				this.buffer.flip();
				byte[] tmp = new byte[this.buffer.limit()];
				this.buffer.get(tmp);
				this.buffer.clear();
				this.buffer.put(data);
				long start = System.currentTimeMillis();
				new WriteXmlFileJob(tmp).run(); //FIXME TODO �����̳߳�ʵ�֣���ʱ��������
				logger.warn("д�ļ���ʱ " + (System.currentTimeMillis() - start) + "ms �ļ���С :" + tmp.length );
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
	 * ��Buffer�д��������flush������ 
	 */
	protected class FinishTask implements Runnable{
		public void run() {
			if(buffer.position() > 0){
				logger.warn("[FULL] ==> ����������ɨβ��������Buffer�е�����Flush�������С�");
				buffer.flip();
				byte[] tmp = new byte[buffer.limit()];
				buffer.get(tmp);
				buffer.clear();
				new WriteXmlFileJob(tmp).run();//FIXME TODO �����̳߳�ʵ�֣���ʱ��������
			}

			deletePendingFiles();
			endTime = new Date();
			logger.warn("[FULL] ==> �����˴�ȫ������ʼʱ��Ϊ:" + startTime + "����ʱ��Ϊ:" + endTime + " ����ʱ :[" + (endTime.getTime() - startTime.getTime())/1000 + "] ��");
		}
	}
}
