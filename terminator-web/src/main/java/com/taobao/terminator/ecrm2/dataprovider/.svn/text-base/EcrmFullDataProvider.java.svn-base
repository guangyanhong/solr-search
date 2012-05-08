package com.taobao.terminator.ecrm2.dataprovider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;

import com.taobao.terminator.common.data.DataProvider;

public class EcrmFullDataProvider implements DataProvider,InitializingBean{
	protected Log logger = LogFactory.getLog(EcrmFullDataProvider.class);
	
	protected final static String DEFAULT_FIELD_SEQUENCE = "id,b_id,s_id,nick,count,am,i_c,c_n,l_t,level,source,n_c,n_a,status,group,g_m,bir,sex,pro,city";
	
	private String shellPath = "/home/admin/tools/script/testOk.sh";
	private String dataPath  = "/home/admin/tools/script/file/";
	private String untilTime = "06:00:00";
	private String delimiter = ",";
	private String groupNum  = null;
	private int   buffersize = 1024 * 1024 * 5;
	private String         fieldSequence;
	private List<String>   fieldSequenceList ;
	private String         sequenceDeli = ",";
	
	private File[]         dataFiles;
	private int            currentFileIndex = 0;
	private BufferedReader bufReader;
	private String         line;
	
	private int count = 0;
	
	//fww add 判断重复数据
	private Set<String> set = null;
	
	public Set<String> getSet() {
		return set;
	}

	public void setSet(Set<String> set) {
		this.set = set;
	}

	public void init() throws Exception {
		set = new HashSet<String>();
		
		logger.warn("触发云梯dump数据过程:" + new Date());
		final int rc = callShell();
		if(rc != 0) {
			throw new RuntimeException("Execute Shell ERROR!");
		}
		this.dataFiles = Utils.listDataFiles(dataPath);
		this.currentFileIndex = 0;
		
		this.bufReader = initFileReader();
		logger.warn("开始执行建索引过程，请耐心稍等");
	}

	public boolean hasNext() throws Exception {
		if((this.line = bufReader.readLine())== null) {
			this.bufReader.close();
			
			if(++this.currentFileIndex >= dataFiles.length) {
				return false;
			} else {
				this.bufReader = initFileReader();
				this.line = this.bufReader.readLine();
				return true;
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
	
	protected Map<String, String> generatorMap(String[] strs) {
		count ++;
		if (strs.length != fieldSequenceList.size()) {
			throw new RuntimeException("当前行与给定schema字段个数不相符，忽略"+strs);
		}
		
		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < fieldSequenceList.size(); i++) {
			map.put(fieldSequenceList.get(i), strs[i]);
		}
		checkMapValues(map);
		
		return map;
	}

	public void close() throws Exception {
		set = null;
		count = 0;
		this.currentFileIndex = 0;
		this.dataFiles = null;
		this.line = null;
		
		try {
			if(this.bufReader!= null) {
				this.bufReader.close();
			}
		} finally {
			bufReader = null;
		}
	}
	
	private BufferedReader initFileReader() throws Exception{
		if(dataFiles == null || dataFiles.length == 0){
			logger.debug("云梯dump完成，但没有生成数据文件");
			throw new RuntimeException("云梯dump完成,生成数据文件错误"); 
		}
		FileInputStream inputStream  = new FileInputStream(dataFiles[currentFileIndex]);
		InputStreamReader fileReader = new InputStreamReader(inputStream,"utf-8");
		return new BufferedReader(fileReader, buffersize);
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
	
	
	private void checkMapValues(Map<String, String> map){
		if(set.contains(map.get("id"))){
			logger.error("当前数据有重复" + map.get("id"));
		}else{
			set.add(map.get("id"));
		}
		
		// 计算平均客单价
		long apValue = Long.valueOf(map.get("count"));
		if (apValue == 0) {
			map.put("a_p", 0 + "");
		} else {
			long am = Long.valueOf(map.get("am"));
			map.put("a_p" , Math.round(am * 1d /apValue) + ""); 
		}
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
		
		public void run(){
			Runtime run = Runtime.getRuntime();
			try {
				logger.warn("开始云梯dump过程 " + new Date());
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
		return buffersize;
	}

	public void setBufferSize(int bufferSize) {
		if(bufferSize <= 0) {
			throw new IllegalArgumentException("bufferSize lessThan zero.");
		}
		this.buffersize = bufferSize;
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
				public boolean accept(File file) {
					String fn = file.getName();
					return !fn.endsWith("ok");
				}
			});
		}
		
		public static File[] listDataFiles(String baseDir) {
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
			String currentDate = format.format(new Date());
			return listDataFiles(baseDir, currentDate);
		}
	}
}


