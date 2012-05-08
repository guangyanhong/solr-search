package com.taobao.terminator.ecrm2.fulltimer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Branch;

import com.ibm.icu.text.SimpleDateFormat;
import com.taobao.terminator.core.realtime.FullTimer;

public class EcrmFullTimer implements FullTimer {
	protected Log logger = LogFactory.getLog(EcrmFullTimer.class);
	private final static String SEP = File.separator;
	private final static String DEFAULTFILEPATH = "/home/admin/tools/script/file";
	private String filePath = null; //��ע��
	FileReader fileReader = null;
	BufferedReader br = null;
	
	public long getTime() {
		Date date = null;
		if (StringUtils.isBlank(filePath)) {
			filePath = DEFAULTFILEPATH;
		}
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyMMdd");
			String nowDay = format.format(new Date());
			String nowDayFilePath = filePath + SEP + nowDay + "ok";

			File file  = new File(nowDayFilePath);
			fileReader = new FileReader(file);
			br = new BufferedReader(fileReader);
			String str = null;
			
			if ((str = br.readLine()) == null) {
				throw new RuntimeException("����dump������ɵ�OK�ļ�������");
			} else {
				String[] strs = str.split(" ");
				if (strs != null && strs.length == 2) {
					String endTime = strs[1];
					SimpleDateFormat format1 = new SimpleDateFormat("yyyyMMddHHmmss");
					try {
						date = format1.parse(endTime);
					} catch (ParseException e) {
						throw new RuntimeException("ok�ļ�����ʱ��ת���쳣", e);
					}
					return date.getTime();
				}else{
					throw new RuntimeException("����dump������ɵ�OK�ļ����ݲ���ȷ");
				}
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException("����dump�������,���ɵ�dump�����ļ�������", e);
		} catch (Exception e) {
			throw new RuntimeException("����dump�������,��ʼ�������ļ�ʧ��", e);
		}finally{
			try {
				if(this.br!= null) {
					try {
						this.br.close();
					} catch (IOException e) {
						throw new RuntimeException("�ر�ʱ���ļ��쳣",e);
					}
				}
			} finally {
				br = null;
			}
		}
		
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
	
	public static void main(String[] args) {
		EcrmFullTimer fullTimer = new EcrmFullTimer();
		fullTimer.setFilePath("D:\\terminator\\��Ŀ\\�̻�ƽ̨-ecrm");
		long time = fullTimer.getTime();
		System.out.println(time);
	}

}
