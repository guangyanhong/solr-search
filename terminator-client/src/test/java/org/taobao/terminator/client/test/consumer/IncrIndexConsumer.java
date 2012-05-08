package org.taobao.terminator.client.test.consumer;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.taobao.terminator.common.constant.IndexEnum;

public class IncrIndexConsumer extends AbstractIndexConsumer {
	
	public IncrIndexConsumer(File indexDir) {
		super(indexDir);
		this.threadPool = new ThreadPoolExecutor(this.numOfJobs, this.numOfJobs, 
				Long.MAX_VALUE, TimeUnit.NANOSECONDS, new LinkedBlockingQueue<Runnable>());
	}

	public IncrIndexConsumer(String indexDir) {
		super(indexDir);
		this.threadPool = new ThreadPoolExecutor(this.numOfJobs, this.numOfJobs, 
				Long.MAX_VALUE, TimeUnit.NANOSECONDS, new LinkedBlockingQueue<Runnable>());
	}
	
	public IncrIndexConsumer(String indexDir, int numOfJobs){
		super(indexDir, numOfJobs);
		this.threadPool = new ThreadPoolExecutor(this.numOfJobs, this.numOfJobs, 
				Long.MAX_VALUE, TimeUnit.NANOSECONDS, new LinkedBlockingQueue<Runnable>());
	}
	
	public IncrIndexConsumer(File indexDir, int numOfJobs){
		super(indexDir, numOfJobs);
		this.threadPool = new ThreadPoolExecutor(this.numOfJobs, this.numOfJobs, 
				Long.MAX_VALUE, TimeUnit.NANOSECONDS, new LinkedBlockingQueue<Runnable>());
	}
	
	protected void doFinish(){
		logger.warn("本次增量索引构建完成，没有被成功导入的索引文件有<" + this.pendingFiles.size() + ">个。pendings:" + this.pendingFiles);
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
		logger.debug("Increment dump finished..开始时间：" + this.startTime + "结束时间：" + this.endTime);
	}
	
	public boolean start(){
		this.startTime = new Date();
		return true;
	}
	
	public boolean finish() {
		this.doFinish();
		return true;
	}

	public void consum(byte[] data, String filePath){
		logger.debug("IncrIndexConsumer接收到索引原始数据，直接写入文件。");
		this.threadPool.execute(new WriterIndexJob(data,filePath));
	}
	/**
	 * 返回要写入的索引xml文件的路径，包括文件名
	 */
	protected synchronized String getFilePath(String suffix) throws Exception {
		Thread.sleep(50);
		Calendar calendar = Calendar.getInstance();
		
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		
		StringBuilder sb = new StringBuilder();
		File first = new File(this.indexDir, sb.append(year).append("-").append(month).append("-").append(day).toString());
		File second = new File(first, String.valueOf(hour));
		File third = new File(second, String.valueOf(minute));
		
		if(!first.exists()){
			first.mkdir();
		}
		
		if(!second.exists()){
			second.mkdir();
		}
		
		if(!third.exists()){
			third.mkdir();
		}
		
		SimpleDateFormat formater = new SimpleDateFormat(IndexEnum.DATE_TIME_PATTERN.getValue());
		
		return new StringBuilder().append(this.indexDir.getAbsolutePath()).append(File.separator) 
		.append(sb.toString()).append(File.separator).append(String.valueOf(hour)) 
		.append(File.separator).append(String.valueOf(minute)).append(File.separator) 
		.append(formater.format(calendar.getTime())).append(suffix).toString();
	}

/*	public void receiveIndex(IndexComingEvent event) {
		if(IndexEnum.INCREMENT_MODE.equals(event.getType())){
			super.consum(event.getData());
		} else{
			logger.warn("索引类型不是增量索引，IncrIndexConsumer不对数据进行处理。");
		}
	}*/

}
