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
	private String filePath = null; //可注入
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
				throw new RuntimeException("云梯dump完成生成的OK文件无内容");
			} else {
				String[] strs = str.split(" ");
				if (strs != null && strs.length == 2) {
					String endTime = strs[1];
					SimpleDateFormat format1 = new SimpleDateFormat("yyyyMMddHHmmss");
					try {
						date = format1.parse(endTime);
					} catch (ParseException e) {
						throw new RuntimeException("ok文件结束时间转换异常", e);
					}
					return date.getTime();
				}else{
					throw new RuntimeException("云梯dump完成生成的OK文件内容不正确");
				}
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException("云梯dump数据完成,生成的dump数据文件不存在", e);
		} catch (Exception e) {
			throw new RuntimeException("云梯dump数据完成,初始化数据文件失败", e);
		}finally{
			try {
				if(this.br!= null) {
					try {
						this.br.close();
					} catch (IOException e) {
						throw new RuntimeException("关闭时间文件异常",e);
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
		fullTimer.setFilePath("D:\\terminator\\项目\\商户平台-ecrm");
		long time = fullTimer.getTime();
		System.out.println(time);
	}

}
