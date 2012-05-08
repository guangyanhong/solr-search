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
				//�����ϲ����׳�����쳣�ġ���
				logger.error("ָ���ļ�¼ʱ���ļ�������:"+this.path, fnfe);
				throw new TimeManageException("ָ���ļ�¼ʱ���ļ�������:"+this.path, fnfe);
			} catch (IOException ioe) {
				logger.error("��ȡʱ���ļ������쳣��"+this.path, ioe);
				throw new TimeManageException("��ȡʱ���ļ������쳣��"+this.path, ioe);
			} catch (ParseException pe) {
				logger.error("�����ļ��д洢��ʱ��ʧ�ܣ�", pe);
				throw new TimeManageException("�����ļ��д洢��ʱ��ʧ�ܣ�", pe);
			} finally {
				if(reader!=null) {
					try {
						reader.close();
					} catch (IOException ioe) {
						logger.error("��ȡ�ļ���ر��ļ������쳣", ioe);
					}
				}
			}
		} else {
			//��һ�μ�¼����ʱ�䣬�Ե����0����Ϊ��ʼʱ��
			String d = df.format(this.endTime);
			try {
				this.startTime = TerminatorCommonUtils.parseDate(d + " 00:00:00");
			} catch (ParseException pe) {
				logger.error("�����µ�startTimeʧ��", pe);
				throw new TimeManageException("�����µ�startTimeʧ��", pe);
			}
		}
		inited = true;
		return new StartAndEndTime(this.startTime,this.endTime);
	}

	@Override
	public StartAndEndTime justGetTimes() throws TimeManageException {
		if(!inited) 
			throw new TimeManageException("ʱ�仹δ��ʼ������ȷ���Ѿ�������initTimes()����.");
		return new StartAndEndTime(this.startTime,this.endTime);
	}

	@Override
	public StartAndEndTime resetTimes() throws TimeManageException {
		if(!inited) 
			throw new TimeManageException("ʱ�仹δ��ʼ������ȷ���Ѿ�������initTimes()����.");
		logger.warn("��ʼ��������ʼʱ���¼Ϊ��"+TerminatorCommonUtils.formatDate(this.endTime)
						+"�� �����ļ���"+this.path);
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(new File(this.path)));
			writer.write(TerminatorCommonUtils.formatDate(this.endTime));
			writer.flush();
		} catch (IOException ioe) {
			logger.error("д����������ʱ��ʧ��", ioe);
			throw new TimeManageException("д����������ʱ��ʧ��", ioe);
		} finally {
			if(writer!=null) {
				try {
					writer.close();
				} catch (IOException ioe) {
					logger.error("д���ļ���رշ����쳣��", ioe);
				}
			}
		}
		return this.justGetTimes();
	}

}
