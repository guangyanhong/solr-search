package com.taobao.terminator.web.perftest.dump.ecrm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;

import com.ibm.icu.text.SimpleDateFormat;
import com.taobao.terminator.common.data.DataProvider;

public class EcrmFullDataProvider implements DataProvider,InitializingBean{
	protected Log logger = LogFactory.getLog(EcrmFullDataProvider.class);
	
	protected final static String DEFAULT_FIELD_SEQUENCE = "idb_id,s_id,nick,count,am,i_c,c_n,l_t,level,source,n_c,n_a,status,group,g_m,bir,sex,pro,city";
	
	private String shellPath = "/home/admin/tools/script/testOk.sh";
	private String dataPath  = "/home/admin/tools/script/file/";
	private String untilTime = "06:00:00";
	private String delimiter = ",";
	private String groupNum  = null;
	
	private String         fieldSequence;
	private List<String>   fieldSequenceList ;
	private String         sequenceDeli = ",";
	
	private File[]         dataFiles;
	private int            currentFileIndex = 0;
	private BufferedReader bufReader;
	private String         line;
	
	private int            bufferSize = 1024 * 1024 * 5;

	public void init() throws Exception {
		final int rc = callShell();
		if(rc != 0) {
			throw new RuntimeException("Execute Shell ERROR!");
		}
		this.dataFiles = Utils.listDataFiles(dataPath);
		this.currentFileIndex = 0;
		this.bufReader = new BufferedReader(new FileReader(dataFiles[currentFileIndex]),bufferSize);
	}

	public boolean hasNext() throws Exception {
		if((this.line = bufReader.readLine())== null) {
			if(++this.currentFileIndex >= dataFiles.length) {
				this.bufReader.close();
				return false;
			} else {
				this.bufReader.close();
				this.bufReader = new BufferedReader(new FileReader(dataFiles[currentFileIndex]),bufferSize);
				return this.hasNext();
			}
		}
		return true;
	}

	public Map<String, String> next() throws Exception {
		return generatorMap(this.spliteRow(this.line));
	}
	
	protected String[] spliteRow(String row) {
		String str = row.substring(row.indexOf("\t") + 1);
		return str.split(delimiter);
	}
	
	private Map<String, String> generatorMap(String[] strs) {
		if (strs.length != fieldSequenceList.size()) {
			return null;
		}

		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < fieldSequenceList.size(); i++) {
			map.put(fieldSequenceList.get(i), strs[i]);
		}

		long count = Long.valueOf(map.get("count"));
		if (count == 0) {
			map.put("a_p", 0 + "");
		} else {
			long am = Long.valueOf(map.get("am"));
			map.put("a_p", (am / (count)) + "");
		}
		return map;
	}

	public void close() throws Exception {
		this.currentFileIndex = 0;
		this.dataFiles = null;
		this.line = null;
		
		try {
			if(this.bufReader!= null) {
				this.bufReader.close();
			}
		} finally {
			this.clearOldFiles();
		}
	}
	
	protected void clearOldFiles() throws Exception{
		//TODO
	}

	private int callShell() throws Exception {
		long timeout = Utils.computeDistanceTime(this.untilTime);
		final CountDownLatch latch = new CountDownLatch(1);
		CallShellJob job = new CallShellJob(latch);
		Thread t = new Thread(job,"CALL-SHELL-THREAD");
		t.start();
		latch.await(timeout,TimeUnit.MILLISECONDS);
		final int resultCode = job.resultCode;
		
		if(resultCode == Integer.MIN_VALUE) { //在规定时间内没有完成
			this.processTimeout();
		}
		
		if(resultCode == Integer.MAX_VALUE) {//在执行过程中出现异常
			this.processException();
		}
		
		return resultCode;
	}
	
	protected void processTimeout() throws Exception{
		throw new RuntimeException("Call Shell Timeout ERROR!");
	}
	
	protected void processException() throws Exception {
		throw new RuntimeException("Call Shell Thrown Exception.");
	}
	
	protected class CallShellJob implements Runnable {
		private CountDownLatch latch;
		private Process process;
		volatile private int resultCode = Integer.MIN_VALUE;
		
		private CallShellJob(CountDownLatch latch) {
			this.latch = latch;
		}
		
		@Override
		public void run(){
			Runtime run = Runtime.getRuntime();
			try {
				this.process = run.exec(shellPath + " " + groupNum);
				this.resultCode = process.waitFor();
			} catch(Exception e){
				this.resultCode = Integer.MAX_VALUE;
				throw new RuntimeException("Call Shell ERROR!",e);
			} finally {
				latch.countDown();
			}
		}
		
		protected void terminalShell() {
			this.process.destroy();
		}
	}
	
	public void setFieldSequence(String seq) {
		this.fieldSequence = seq;
		String[] fields = seq.split(sequenceDeli);
		if(fields == null || fields.length <= 0) {
			throw new IllegalArgumentException("fieldSequence format ERROR. {" + seq +"}");
		}
		
		if(fieldSequenceList != null && !fieldSequenceList.isEmpty()) {
			fieldSequenceList.clear();
		} else {
			fieldSequenceList = new ArrayList<String>();
		}
		
		for(String field : fields) {
			fieldSequenceList.add(field.trim());
		}
	}
	
	public List<String> getFieldSequenceList() {
		return fieldSequenceList;
	}

	public void setFieldSequenceList(List<String> fieldSequenceList) {
		this.fieldSequenceList = fieldSequenceList;
	}

	public String getFieldSequence() {
		return fieldSequence;
	}

	public void setCoreName(String coreName) {
		String[] strs = coreName.split("-");
		if (strs != null && strs.length == 2) {
			this.groupNum = strs[1];
		} else {
			throw new IllegalArgumentException("CoreName Formate ERROR ,{ " + coreName +" }");
		}
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		if(this.groupNum == null) {
			throw new IllegalStateException("groupNum is NULL.");
		}
		
		if(!new File(this.shellPath).exists()) {
			throw new FileNotFoundException("Shell doesn't exist {" + this.shellPath +"}");
		}
		
		if(fieldSequenceList == null || fieldSequenceList.isEmpty()) {
			this.setFieldSequence(DEFAULT_FIELD_SEQUENCE);
		}
	}
	
	public String getShellPath() {
		return shellPath;
	}

	public void setShellPath(String shellPath) {
		this.shellPath = shellPath;
	}

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	public String getUntilTime() {
		return untilTime;
	}

	public void setUntilTime(String untilTime) {
		this.untilTime = untilTime;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		if(bufferSize <= 0) {
			throw new IllegalArgumentException("bufferSize lessThan zero.");
		}
		this.bufferSize = bufferSize;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public String getGroupNum() {
		return groupNum;
	}

	public void setGroupNum(String groupNum) {
		this.groupNum = groupNum;
	}

	public String getSequenceDeli() {
		return sequenceDeli;
	}

	public void setSequenceDeli(String sequenceDeli) {
		this.sequenceDeli = sequenceDeli;
	}

	public static class Utils {
		
		public static long computeDistanceTime(String timeDesc) {
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
			String nowDay = format.format(new Date());
			StringBuilder builder = new StringBuilder();
			builder.append(nowDay + " ");
			builder.append(timeDesc);
			format = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
			Date timeoutDate = null;
			
			try {
				timeoutDate = format.parse(builder.toString());
			} catch (ParseException e) {
				throw new RuntimeException("Ecrm全量dump初始化timeOut日期转换错误", e);
			}

			long distanceTime = timeoutDate.getTime() - new Date().getTime();
			if (distanceTime <= 0) {
				throw new RuntimeException("设定dump超时时间小于当前时间,请重新设置");
			}
			return distanceTime;
		}
		
		public static File[] listDataFiles(String baseDir,String subDir) {
			File dir = new File(baseDir,subDir);
			return dir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File file) {
					String fn = file.getName();
					return !fn.endsWith("ok");
				}
			});
		}
		
		public static File[] listDataFiles(String baseDir) {
			Calendar ca = Calendar.getInstance();
			String currentDate = new StringBuilder().append(ca.get(Calendar.YEAR)).append(ca.get(Calendar.MONDAY)).append(ca.get(Calendar.DAY_OF_MONTH)).toString();
			return listDataFiles(baseDir, currentDate);
		}
	}
}
