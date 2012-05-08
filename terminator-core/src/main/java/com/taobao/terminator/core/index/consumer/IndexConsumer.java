package com.taobao.terminator.core.index.consumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.constant.IndexEnum;

public abstract class IndexConsumer{
	protected static Log logger = LogFactory.getLog(IndexConsumer.class);
	
	protected Map<String, File> pendingFiles;
	protected File indexDir;

	protected ThreadPoolExecutor threadPool;
	protected int jobnum = 5;

	protected Date startTime;
	protected Date endTime;
	
	public IndexConsumer(){}
	
	public IndexConsumer(File indexDir, int jobnum){
		if(indexDir == null){
			throw new IllegalArgumentException("Invalid indexDir:indexDir can't be null or empty.");
		}
		this.indexDir = indexDir;
		if(!indexDir.isDirectory()){
			throw new IllegalArgumentException("Invalid indexDir:indexDir must be a directory.");
		}
		this.jobnum = jobnum;
		this.init();
	}

	public void init(){
		this.startTime    = null;
		this.endTime      = null;
		this.pendingFiles = new HashMap<String, File>();
		this.threadPool   = new ThreadPoolExecutor(this.jobnum, this.jobnum, Long.MAX_VALUE, TimeUnit.NANOSECONDS, new LinkedBlockingQueue<Runnable>());
	}
	
	public abstract void start();

	public abstract void consum(byte[] data);
	
	public abstract void finish();
	
	protected abstract String getFilePath(String fileSuffix) throws Exception;
	
	protected void deletePendingFiles(){
		logger.warn("本次全量索引构建完成，没有被成功写入的索引文件有<" + pendingFiles.size() + ">个，删除有误的文件.pendings:" + pendingFiles);
		for(Map.Entry<String, File> entry : pendingFiles.entrySet()){
			if(entry.getValue().exists() && entry.getValue().delete()){
				logger.warn("删除错误的索引文件成功，file:" + entry.getValue().getName());
			} else{
				logger.warn("删除错误的索引文件失败，file:" + entry.getValue().getName());
			}
		}
		pendingFiles.clear();
	}
	
	protected class WriteXmlFileJob implements Runnable{
		private byte[] data;

		WriteXmlFileJob(byte[] data){
			this.data = data;
		}
		
		private String rename(String tmpFileName){
			return tmpFileName.replaceAll(IndexEnum.TMP_INDEX_FILE_SUFFIX.getValue(),IndexEnum.INDEX_FILE_SUFFIX.getValue());
		}
		
		public void run() {
			String filePath = null;
			try {
				filePath = getFilePath(IndexEnum.TMP_INDEX_FILE_SUFFIX.getValue());
			} catch (Exception e) {
				logger.error("获取XML文件名失败，忽略本次数据。。。", e);
				return;
			}
			logger.warn("写XML文件 Path ==> " + filePath);
			
			File tmpIndexFile = new File(filePath);
			BufferedWriter out = null;
			try {
				out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpIndexFile), "GBK"));
			} catch (FileNotFoundException e) {
				logger.error("打开文件失败.file:" + tmpIndexFile.getAbsolutePath(), e);
				throw new IllegalArgumentException("Can't open file. file:" + tmpIndexFile.getAbsolutePath());
			} catch (UnsupportedEncodingException e) {
				logger.error("JAVA版本不支持该字符集:GBK", e);
				throw new IllegalArgumentException("Unsupported Encoding:UTF-8");
			}
			
			try {
				//添加一个外围的START_ELEMENT，否则xml解析时将报错
				out.write("<?xml version=\"1.0\" encoding=\"GBK\"?>");
				out.write("<docs>");
				out.write(new String(this.data));
				//添加一个外围的END_ELEMENT，否则xml解析时将报错
				out.write("</docs>");
			} catch (IOException e) {
				logger.error("将索引数据写入文件时出错！", e);
				addPending(filePath, tmpIndexFile);
				return;
			} finally{
				try {
					out.close();
				} catch (IOException e) {
					logger.error("关闭文件失败,file:" + tmpIndexFile.getAbsolutePath(), e);
				}
			}
			
			if(!tmpIndexFile.renameTo(new File(this.rename(filePath)))){
				logger.error("将文件" + tmpIndexFile.getAbsolutePath() +  "重命名为" + "" + "失败！");
				addPending(filePath, tmpIndexFile);
			} 
		}
	}	

	
	protected synchronized void addPending(String fileName, File file){
		this.pendingFiles.put(fileName, file);
	}

	public Map<String, File> getPendingFiles() {
		return pendingFiles;
	}

	public void setPendingFiles(Map<String, File> pendingFiles) {
		this.pendingFiles = pendingFiles;
	}

	public File getIndexDir() {
		return indexDir;
	}

	public void setIndexDir(File indexDir) {
		this.indexDir = indexDir;
	}

	public ThreadPoolExecutor getThreadPool() {
		return threadPool;
	}

	public void setThreadPool(ThreadPoolExecutor threadPool) {
		this.threadPool = threadPool;
	}

	public int getJobnum() {
		return jobnum;
	}

	public void setJobnum(int jobnum) {
		this.jobnum = jobnum;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}
}
