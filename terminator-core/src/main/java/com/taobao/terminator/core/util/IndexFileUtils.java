package com.taobao.terminator.core.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.constant.IndexEnum;

public class IndexFileUtils {
	private static Log logger = LogFactory.getLog(IndexFileUtils.class);
	private static SimpleDateFormat formater = new SimpleDateFormat(IndexEnum.DATE_TIME_PATTERN.getValue());
	private static DecimalFormat numberFormater = new DecimalFormat("00");
	
	public static Map<Integer, String> monthMap;
	
	static{
		monthMap = new HashMap<Integer, String>();
		monthMap.put(Calendar.JANUARY, "01");
		monthMap.put(Calendar.FEBRUARY, "02");
		monthMap.put(Calendar.MARCH, "03");
		monthMap.put(Calendar.APRIL, "04");
		monthMap.put(Calendar.MAY, "05");
		monthMap.put(Calendar.JUNE, "06");
		monthMap.put(Calendar.JULY, "07");
		monthMap.put(Calendar.AUGUST, "08");
		monthMap.put(Calendar.SEPTEMBER, "09");
		monthMap.put(Calendar.OCTOBER, "10");
		monthMap.put(Calendar.NOVEMBER, "11");
		monthMap.put(Calendar.DECEMBER, "12");
	}
	
	public static String formatDate(Date date){
		return formater.format(date);
	}
	
	/**
	 * 从指定目录下，返回指定数量的索引原始数据文件
	 * 
	 * @param dir
	 *            索引原始数据的存放目录
	 * @param numOfFiles
	 *            返回的文件大小
	 * @return 没有索引原始数据文件时，返回null
	 */
	public static Collection<File> listFile(File dir, FileFilter filter,boolean isRecursive) {
		if (!dir.isDirectory()) {
			throw new IllegalArgumentException( "Parameter 'directory' is not a directory");
		}
		if (filter == null) {
			throw new NullPointerException("Parameter 'fileFilter' is null");
		}

		Collection<File> files = new java.util.LinkedList<File>();
		interListFiles(files, dir, filter, isRecursive);

		return files;
	}
	/**
	 * 这个方法用来在增量xml文件夹中找增量xml文件。在找的过程中，只会去找比穿的fileName更新的文件。
	 * 同时，会根据fileName，跳过不必要的文件夹。
	 * @param rootDir
	 * 	增量XML文件夹的根目录
	 * @param startDate
	 * 	比startDate更新的文件将会被返回
	 * @return
	 * @throws ParseException 
	 */	
	public static Collection<File> listIncrFile(File rootDir, String startDate, int maxFileNum) throws ParseException{
		
		SimpleDateFormat formater = new SimpleDateFormat(IndexEnum.DATE_TIME_PATTERN.getValue());
		DecimalFormat numberFormater = new DecimalFormat("00");
		Collection<File> files = new LinkedList<File>();
		
		if(startDate == null || "".equals(startDate)){
			logger.warn("开始时间为null或者空，故从头取文件");
			FullIndexFileFilter fileFilter = new FullIndexFileFilter(maxFileNum, null);
			files = IndexFileUtils.listFile(rootDir, fileFilter, true);
			return files;
		}
		
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(formater.parse(startDate));
		
		String dayDir    = calendar.get(Calendar.YEAR) + "-" + IndexFileUtils.monthMap.get(calendar.get(Calendar.MONTH)) + "-" + numberFormater.format(calendar.get(Calendar.DAY_OF_MONTH));
		String hourDir   = numberFormater.format(calendar.get(Calendar.HOUR_OF_DAY));
		String minuteDir = numberFormater.format(calendar.get(Calendar.MINUTE));
		
		IncrIndexFileFilter fileFilter = new IncrIndexFileFilter(startDate, maxFileNum);
		
		//首先找出fileName对应的目录下比fileName时间更新的所有文件
		interListFiles(files, new File(rootDir, dayDir + File.separator + hourDir + File.separator + minuteDir), fileFilter, false);
		if(files.size() == maxFileNum){
			//如果已经达到返回文件数的最大值
			return files;
		}
		
		//找到所有在分这一级比fileName这个文件所处的分这一级文件夹大的所有文件夹
		files.addAll(findFiles(new File(rootDir, dayDir + File.separator + hourDir), 
				minuteDir, maxFileNum - files.size(), fileFilter, false));
		if(files.size() == maxFileNum){
			return files;
		}
		
		//找到所有在小时这一级比fileName这个文件所处的小时这一级文件夹大的所有文件夹
		files.addAll(findFiles(new File(rootDir, dayDir), 
				hourDir, maxFileNum - files.size(), fileFilter, true));
		if(files.size() == maxFileNum){
			return files;
		}
		
		//找到所有在天这一级比fileName这个文件所处的天这一级文件夹大的所有文件夹
		files.addAll(findFiles(rootDir, dayDir, maxFileNum - files.size(), fileFilter, true));
		
		return files;
	}
	
	/**
	 * 从给定的文件名中找到这个文件所在的文件夹。由于增量索引被按时间放在相应的文件夹中，而文件名是该 xml文件
	 * 被创建的时间，索引可以通过文件名找到该文件所在的文件夹。
	 * 增量xml文件被存放成三级目录，第一级以年-月-日命名，第二级以小时命名，第三级以分命名
	 * @param baseDir
	 * @param fileName
	 * @param suffix
	 * @return
	 * @throws ParseException
	 */
	public static String generatePathFromFileName(File baseDir,String fileName,String suffix) throws ParseException{
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(formater.parse(fileName));
		
		int year = calendar.get(Calendar.YEAR);
		String month = monthMap.get(calendar.get(Calendar.MONTH));
		String day = numberFormater.format(calendar.get(Calendar.DAY_OF_MONTH));
		String hour = numberFormater.format(calendar.get(Calendar.HOUR_OF_DAY));
		String minute = numberFormater.format(calendar.get(Calendar.MINUTE));
		
		StringBuilder sb = new StringBuilder();
		File first = new File(baseDir, sb.append(year).append("-").append(month).append("-").append(day).toString());
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
		
		return new StringBuilder().append(baseDir.getAbsolutePath()).append(File.separator) 
		.append(sb.toString()).append(File.separator).append(hour) 
		.append(File.separator).append(minute).append(File.separator) 
		.append(formater.format(calendar.getTime())).append(suffix).toString();
	}
	
	private static Collection<File> findFiles(File baseDir, String baseFileName, 
			int maxNumOfFiles, IndexFileFilter filter, boolean isRecursive){
		Collection<File> files = new LinkedList<File>();
		String[] dirs = baseDir.list();
		if(dirs != null){
			for(String dir : dirs){
				if(new File(baseDir, dir).isDirectory()){
					if(dir.compareTo(baseFileName) > 0){
						filter.resetCount();
						filter.setMaxNumOfFiles(maxNumOfFiles - files.size());
						
						interListFiles(files, new File(baseDir, dir), filter, isRecursive);
						if(files.size() == maxNumOfFiles){
							return files;
						}
					}
				}
			}
		}
		
		return files;
	}
	
	/**
	 * 全量结束之后要修改一下增量开始时间文件，用来告诉增量索引，下一次增量从那个文件开始 我们的索引文件都是以时间作为文件名的
	 * 如果是第一次全量，那么就会创建这个增量开始时间文件。增量索引的IncrIndexFileSearcher
	 * 每次开始都会读取这个文件，如果没有这个文件那么增量任务就 直接结束了。否则就根据这个文件
	 * 中的时间找到在这个时间点之后创建的增量xml文件，然后进行索引。IncrIndexFileSearcher会找到
	 * 所有的可用文件，然后返回，返回之前会将最后一个，也就是时间最大的那个文件的文件名记录到 增量开始时间文件。
	 * 
	 * @param rootDir
	 * @param date
	 * 	下一次增量开始的时间，这个参数要求传入整个文件名，包括后缀
	 */
	public static void writeIncrStartTimeToFile(File rootDir, String date) {
		File incrStartTimeFile = new File(rootDir,
				IndexEnum.INCR_START_TIME_FILE.getValue());

		if (!incrStartTimeFile.exists()) {
			try {
				incrStartTimeFile.createNewFile();
			} catch (IOException e) {
				logger.error("创建增量开始时间文件失败。。。", e);
				return;
			}
		}
		FileWriter writer = null;
		int tryNum = 0;
		// 写入时间记录
		while (tryNum < 3) {
			try {
				writer = new FileWriter(incrStartTimeFile);
				writer.write(date);
			} catch (IOException e) {
				tryNum++;
				logger.error("写增量时间开始时间文件失败。。。。第<" + tryNum + ">次", e);
				continue;
			} finally {
				try {
					if (writer != null) {
						writer.close();
					}
				} catch (IOException e) {
					logger.error("关闭写增量时间开始时间文件的FileWriter失败。。。", e);
				}
			}
			break;
		}

		if (tryNum > 0) {
			logger.error("重试写增量时间开始时间文件3次之后仍然失败，不写了。。。。");
		}
	}
	
	/**
	 * 全量结束之后要修改一下增量开始时间文件，用来告诉增量索引，下一次增量从那个文件开始 我们的索引文件都是以时间作为文件名的
	 * 如果是第一次全量，那么就会创建这个增量开始时间文件。增量索引的IncrIndexFileSearcher
	 * 每次开始都会读取这个文件，如果没有这个文件那么增量任务就 直接结束了。否则就根据这个文件
	 * 中的时间找到在这个时间点之后创建的增量xml文件，然后进行索引。IncrIndexFileSearcher会找到
	 * 所有的可用文件，然后返回，返回之前会将最后一个，也就是时间最大的那个文件的文件名记录到 增量开始时间文件。
	 * 
	 * @param rootDir
	 * @param date
	 * 	下一次增量开始的时间，这个参数要求传入整个文件名，包括后缀
	 */
	public static void writeIncrStartTimeToFile(File rootDir, String incrTimeFile, String date) {
		File incrStartTimeFile = new File(rootDir,
				incrTimeFile);

		if (!incrStartTimeFile.exists()) {
			try {
				incrStartTimeFile.createNewFile();
			} catch (IOException e) {
				logger.error("创建增量开始时间文件失败。。。", e);
				return;
			}
		}
		FileWriter writer = null;
		int tryNum = 0;
		// 写入时间记录
		while (tryNum < 3) {
			try {
				writer = new FileWriter(incrStartTimeFile);
				writer.write(date);
			} catch (IOException e) {
				tryNum++;
				logger.error("写增量时间开始时间文件失败。。。。第<" + tryNum + ">次", e);
				continue;
			} finally {
				try {
					if (writer != null) {
						writer.close();
					}
				} catch (IOException e) {
					logger.error("关闭写增量时间开始时间文件的FileWriter失败。。。", e);
				}
			}
			break;
		}

		if (tryNum > 0) {
			logger.error("重试写增量时间开始时间文件3次之后仍然失败，不写了。。。。");
		}
	}
	
	public static void writeIncrStartTimeToFile(String rootDirStr, String incrTimeFile, String date) {
		File rootDir = new File(rootDirStr);
		writeIncrStartTimeToFile(rootDir, incrTimeFile, date);
	}
	
	public static void writeIncrStartTimeToFile(String rootDirStr, String date) {
		File rootDir = new File(rootDirStr);
		writeIncrStartTimeToFile(rootDir,date);
	}
	
	public static void writeSyncXmlFileDate(String rootDirStr,String date){
		File rootDir = new File(rootDirStr);
		writeSyncXmlFileDate(rootDir,date);
	}

	private static void writeSyncXmlFileDate(File rootDir, String date) {

		File incrStartTimeFile = new File(rootDir, IndexEnum.SYNC_XML_FROM_MASTER_TIME.getValue());

		if (!incrStartTimeFile.exists()) {
			try {
				incrStartTimeFile.createNewFile();
			} catch (IOException e) {
				logger.error("创建增量开始时间文件失败。。。", e);
				return;
			}
		}
		FileWriter writer = null;
		int tryNum = 0;
		// 写入时间记录
		while (tryNum < 3) {
			try {
				writer = new FileWriter(incrStartTimeFile);
				writer.write(date);
			} catch (IOException e) {
				tryNum++;
				logger.error("写增量时间开始时间文件失败。。。。第<" + tryNum + ">次", e);
				continue;
			} finally {
				try {
					if (writer != null) {
						writer.close();
					}
				} catch (IOException e) {
					logger.error("关闭写增量时间开始时间文件的FileWriter失败。。。", e);
				}
			}
			break;
		}

		if (tryNum > 0) {
			logger.error("重试写增量时间开始时间文件3次之后仍然失败，不写了。。。。");
		}
	
	}
	private static void interListFiles(Collection<File> files, File dir,FileFilter filter, boolean isRecursive) {
		File[] found = dir.listFiles(filter);

		if (found != null) {
			for (int i = 0; i < found.length; i++) {
				if (found[i].isDirectory()) {
					if (isRecursive) {
						interListFiles(files, found[i], filter, isRecursive);
					}
				} else {
					files.add(found[i]);
				}
			}
		}
	}
	
	/**
	 * 获取增量应该开始的时间。这个时间被记录在一个文件中，该文件位于增量xml文件的根目录下，
	 * 文件名是incr_start_time。这个时间其实就是上一次增量获取的所有文件中，日期最近的一个
	 * 文件的文件名(文件名就是该文件创建的时间)
	 * @param rootDir
	 * 	存放增量索引文件的根目录
	 * @return
	 * @throws IOException
	 */
	public static String getIncreStartDate(File rootDir) throws IOException{
		return getStartDate(rootDir.getAbsolutePath());
	}
	
	/**
	 * 获取增量应该开始的时间。这个时间被记录在一个文件中，该文件位于增量xml文件的根目录下，
	 * 文件名是incr_start_time。这个时间其实就是上一次增量获取的所有文件中，日期最近的一个
	 * 文件的文件名(文件名就是该文件创建的时间)
	 * @param rootDir
	 * 	存放增量索引xml文件的根目录路径
	 * @return
	 * @throws IOException
	 */
	public static String getStartDate(String rootDir) throws IOException{
		BufferedReader reader = null;
		String startFileName = null;
		try{
			File incrStartTimeFile = new File(rootDir,IndexEnum.INCR_START_TIME_FILE.getValue());
			if(!incrStartTimeFile.exists()){
				return "";
			}
			reader = new BufferedReader(new FileReader(incrStartTimeFile));
			startFileName = reader.readLine();
		}finally{
			if(reader != null)
				reader.close();
		}
		return startFileName != null ? startFileName : "";
	}
	
	public static String getSyncXmlFileDate(String rootDir)throws IOException{
		BufferedReader reader = null;
		String 	startTime = null;
		try{
			File incrStartTimeFile = new File(rootDir,IndexEnum.SYNC_XML_FROM_MASTER_TIME.getValue());
			if(!incrStartTimeFile.exists()){
				return "";
			}
			reader = new BufferedReader(new FileReader(incrStartTimeFile));
			startTime = reader.readLine();
		}finally{
			if(reader != null)
				reader.close();
		}	
		return startTime != null ? startTime : "";
	}
	
	/**
	 * 删除增量xml文件目录下面2天以前的目录
	 * @param incrXmlRootDir
	 */
	public static void cleanUpOldIncrXmlFile(File incrXmlRootDir){
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(new Date());
		
		calendar.add(Calendar.DAY_OF_MONTH, -2);
		
		int year = calendar.get(Calendar.YEAR);
		String month = monthMap.get(calendar.get(Calendar.MONTH));
		String day = numberFormater.format(calendar.get(Calendar.DAY_OF_MONTH));
		
		StringBuilder twoDaysAgo = new StringBuilder();
		twoDaysAgo.append(year).append("-").append(month).append("-").append(day).toString();
		
		for(String dir : incrXmlRootDir.list()){
			File tmp = new File(incrXmlRootDir, dir);
			if(tmp.isDirectory()){
				//如果目录名表示的时间在两天以前(包括两天)，那么就删除
				if(tmp.getName().compareTo(twoDaysAgo.toString()) <= 0){
					try {
						IndexUtils.cleanDir(tmp);
					} catch (IOException e) {
						logger.error("清除增量xml数据目录失败，目录为:" + tmp.getName(), e);
					}
				}
				
				tmp.delete();
			}
		}
	}
}
