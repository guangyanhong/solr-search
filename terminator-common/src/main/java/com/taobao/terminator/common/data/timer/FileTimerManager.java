package com.taobao.terminator.common.data.timer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.TerminatorCommonUtils;

public class FileTimerManager implements TimerManager {
	
	protected Log logger = LogFactory.getLog(FileTimerManager.class);
	
	protected String path;
	protected DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	protected Date startTime;
	protected Date endTime;
	protected boolean inited;
	
	public FileTimerManager(String path) {
		this.path = path;
		this.inited = false;
	}

	@Override
	public StartAndEndTime initTimes() throws TimeManageException {
		this.endTime = new Date();
		File timeFile = new File(this.path);
		if(timeFile.exists()) {
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(timeFile));
				String strStartTime = reader.readLine();
				this.startTime = TerminatorCommonUtils.parseDate(strStartTime);
			} catch (FileNotFoundException fnfe) {
				//理论上不会抛出这个异常的……
				logger.error("指定的记录时间文件不存在:"+this.path, fnfe);
				throw new TimeManageException("指定的记录时间文件不存在:"+this.path, fnfe);
			} catch (IOException ioe) {
				logger.error("读取时间文件发生异常："+this.path, ioe);
				throw new TimeManageException("读取时间文件发生异常："+this.path, ioe);
			} catch (ParseException pe) {
				logger.error("解析文件中存储的时间失败！", pe);
				throw new TimeManageException("解析文件中存储的时间失败！", pe);
			} finally {
				if(reader!=null) {
					try {
						reader.close();
					} catch (IOException ioe) {
						logger.error("读取文件后关闭文件发生异常", ioe);
					}
				}
			}
		} else {
			//第一次记录增量时间，以当天的0点作为起始时间
			String d = df.format(this.endTime);
			try {
				this.startTime = TerminatorCommonUtils.parseDate(d + " 00:00:00");
			} catch (ParseException pe) {
				logger.error("解析新的startTime失败", pe);
				throw new TimeManageException("解析新的startTime失败", pe);
			}
		}
		inited = true;
		return new StartAndEndTime(this.startTime,this.endTime);
	}

	@Override
	public StartAndEndTime justGetTimes() throws TimeManageException {
		if(!inited) 
			throw new TimeManageException("时间还未初始化，请确保已经调用了initTimes()方法.");
		return new StartAndEndTime(this.startTime,this.endTime);
	}

	@Override
	public StartAndEndTime resetTimes() throws TimeManageException {
		if(!inited) 
			throw new TimeManageException("时间还未初始化，请确保已经调用了initTimes()方法.");
		logger.warn("开始将增量开始时间记录为："+TerminatorCommonUtils.formatDate(this.endTime)
						+"， 存入文件："+this.path);
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(new File(this.path)));
			writer.write(TerminatorCommonUtils.formatDate(this.endTime));
			writer.flush();
		} catch (IOException ioe) {
			logger.error("写入增量结束时间失败", ioe);
			throw new TimeManageException("写入增量结束时间失败", ioe);
		} finally {
			if(writer!=null) {
				try {
					writer.close();
				} catch (IOException ioe) {
					logger.error("写入文件后关闭发生异常！", ioe);
				}
			}
		}
		return this.justGetTimes();
	}

}
