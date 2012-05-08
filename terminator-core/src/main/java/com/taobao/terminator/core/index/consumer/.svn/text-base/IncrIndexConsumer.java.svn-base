package com.taobao.terminator.core.index.consumer;

import java.io.File;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.taobao.terminator.common.constant.IndexEnum;
import com.taobao.terminator.core.util.IndexFileUtils;

public class IncrIndexConsumer extends IndexConsumer {
	
	public IncrIndexConsumer() {
		super();
	}

	public IncrIndexConsumer(File indexDir, int jobnum) {
		super(indexDir, jobnum);
	}

	public void start(){
		logger.warn("[INCREMENT] ==> 开始一次增量任务请求，接下来会传送N批数据过来。。。");
		this.startTime = new Date();
	}
	
	public void finish() {
		deletePendingFiles();
		this.endTime = new Date();
		logger.warn("[INCREMENT] ==> 结束一次增量任务，开始时间：" + this.startTime + "结束时间：" + this.endTime + " 共耗时 :[" + (endTime.getTime() - startTime.getTime())/1000 + "] 秒");
	}

	public void consum(byte[] data){
		if(data == null || data.length == 0) return;
		logger.warn("[INCREMENT] ==> 接收到增量的xml数据，直接写入文件。");
		new WriteXmlFileJob(data).run(); //FIXME TODO 改用线程池实现，暂时用这个
	}
	
	/**
	 * 返回要写入的索引xml文件的路径，包括文件名
	 */
	protected synchronized String getFilePath(String suffix) throws Exception {
		Thread.sleep(50);
		Calendar calendar = Calendar.getInstance();
		DecimalFormat numberFormater = new DecimalFormat("00");
		
		int year = calendar.get(Calendar.YEAR);
		String month = IndexFileUtils.monthMap.get(calendar.get(Calendar.MONTH));
		String day = numberFormater.format(calendar.get(Calendar.DAY_OF_MONTH));
		String hour = numberFormater.format(calendar.get(Calendar.HOUR_OF_DAY));
		String minute = numberFormater.format(calendar.get(Calendar.MINUTE));
		
		StringBuilder sb = new StringBuilder();
		File first = new File(this.indexDir, sb.append(year).append("-").append(month).append("-").append(day).toString());
		File second = new File(first, hour);
		File third = new File(second, minute);
		
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
		.append(sb.toString()).append(File.separator).append(hour) 
		.append(File.separator).append(minute).append(File.separator) 
		.append(formater.format(calendar.getTime())).append(suffix).toString();
	}
	
}
