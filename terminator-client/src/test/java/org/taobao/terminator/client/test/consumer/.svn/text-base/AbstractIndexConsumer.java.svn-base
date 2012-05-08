package org.taobao.terminator.client.test.consumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.constant.IndexEnum;

public abstract class AbstractIndexConsumer implements IndexConsumer {
	private static final int DEFAULT_NUM_JOBS = 5;
	private static final int BUFFER_SIZE = 1024;
	protected static Log logger = LogFactory.getLog(IndexConsumer.class);
	protected ByteBuffer buffer;
	/**
	 *	记录没有写成功的索引数据文
	 */
	protected Map<String, File> pendingFiles;
	/**
	 * 索引文件目录
	 */
	protected File indexDir;
	/**
	 * 用来异步写索引数据文件的线程池
	 */
	protected ThreadPoolExecutor threadPool;
	/**
	 * 线程池的任务数
	 */
	protected int numOfJobs;
	/**
	 * 记录本次索引开始构建的时间
	 */
	protected Date startTime;
	/**
	 * 记录本次索引的结束时间
	 */
	protected Date endTime;
	
	public AbstractIndexConsumer(String indexDir){		
		this(indexDir, AbstractIndexConsumer.DEFAULT_NUM_JOBS);
	}
	
	public AbstractIndexConsumer(File indexDir){
		this(indexDir, AbstractIndexConsumer.DEFAULT_NUM_JOBS);
	}
	
	public AbstractIndexConsumer(File indexDir, int numOfJobs){
		if(indexDir == null){
			throw new IllegalArgumentException("Invalid indexDir:indexDir can't be null or empty.");
		}
		this.indexDir = indexDir;
		if(!indexDir.isDirectory()){
			throw new IllegalArgumentException("Invalid indexDir:indexDir must be a directory.");
		}
		this.numOfJobs = numOfJobs;
		this.init();
	}
	
	public AbstractIndexConsumer(String indexDir, int numOfJobs){
		if(indexDir == null || "".equals(indexDir)){
			throw new IllegalArgumentException("Invalid indexDir:indexDir can't be null or empty.");
		}
		
		this.indexDir = new File(indexDir);
		
		if(!this.indexDir.isDirectory()){
			throw new IllegalArgumentException("Invalid indexDir:indexDir must be a directory.");
		}
		
		this.numOfJobs = numOfJobs;
		this.init();
	}
	
	public abstract boolean start();
	
	protected abstract void doFinish();
	
	public abstract boolean finish();
	
	private void init(){
		if(this.numOfJobs <= 0){
			this.numOfJobs = AbstractIndexConsumer.DEFAULT_NUM_JOBS;
		}
		
		this.startTime = null;
		this.endTime = null;
		this.pendingFiles = new HashMap<String, File>();
		this.buffer = ByteBuffer.allocate(BUFFER_SIZE);
	}
	
	
	protected class WriterIndexJob implements Runnable{
		private byte[] data;
		private String filePath = null;

		WriterIndexJob(byte[] data,String filePath){
			this.data = data;
			this.filePath = filePath;
		}
		
		private String rename(String tmpFileName){
			return tmpFileName.replaceAll(IndexEnum.TMP_INDEX_FILE_SUFFIX.getValue(), 
					IndexEnum.INDEX_FILE_SUFFIX.getValue());
		}
		
		public void run() {
			//add by yusen
			String fileName = null;
			if(filePath == null){
				try {
					fileName = AbstractIndexConsumer.this.getFilePath(IndexEnum.TMP_INDEX_FILE_SUFFIX.getValue());
				} catch (Exception e) {
					logger.error("获取增量索引文件名失败，忽略本次增量数据。。。", e);
					return;
				}
			}else{
				fileName = filePath;
			}
			File tmpIndexFile = new File(fileName);
			BufferedWriter out = null;
			try {
				out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpIndexFile), "UTF-8"));
			} catch (FileNotFoundException e) {
				logger.error("打开文件失败.file:" + tmpIndexFile.getAbsolutePath(), e);
				throw new IllegalArgumentException("Can't open file. file:" + tmpIndexFile.getAbsolutePath());
			} catch (UnsupportedEncodingException e) {
				logger.error("JAVA版本不支持该字符集:UTF-8", e);
				throw new IllegalArgumentException("Unsupported Encoding:UTF-8");
			}
			
			try {
				//添加一个外围的START_ELEMENT，否则xml解析时将报错
				out.write("<docs>");
				out.write(new String(this.data));
				//添加一个外围的END_ELEMENT，否则xml解析时将报错
				out.write("</docs>");
			} catch (IOException e) {
				logger.error("将索引数据写入文件时出错！", e);
				AbstractIndexConsumer.this.addPending(fileName, tmpIndexFile);
				return;
			} finally{
				try {
					out.close();
				} catch (IOException e) {
					logger.error("关闭文件失败,file:" + tmpIndexFile.getAbsolutePath(), e);
				}
			}
			
			if(!tmpIndexFile.renameTo(new File(this.rename(fileName)))){
				logger.error("将文件" + tmpIndexFile.getAbsolutePath() + 
						"重命名为" + "" + "失败！");
				AbstractIndexConsumer.this.addPending(fileName, tmpIndexFile);
			} 
		}
	}	
	
	/**
	 * 获取要写入的索引xml文件的绝对路径
	 * @param suffix
	 * @return
	 * @throws Exception
	 */
	protected abstract String getFilePath(String fileSuffix) throws Exception;
	
	protected synchronized void addPending(String fileName, File file){
		this.pendingFiles.put(fileName, file);
	}
	
	public void consum(byte[] data,String filePath) {
		logger.debug("接收到索引原始数据，放入buffer");
		//如果有足够的空间来容纳新的data，那么就直接放入buffer
		if(this.buffer.remaining() >= data.length){
			this.buffer.put(data);
		} else{//否则将buffer中的数据写入文件，然后将data放入buffer
			logger.debug("buffer中已经有足够的数据，写入缓存。");
			this.buffer.flip();
			byte[] tmp = new byte[this.buffer.limit()];
			this.buffer.get(tmp);
			this.buffer.clear();
			this.buffer.put(data);
			this.threadPool.execute(new WriterIndexJob(tmp,filePath));
		}
		
		
		//new WriterIndexJob(data,filePath).run();
		logger.debug("启动任完成。。。");
	}

	public Date getStartTime(){
		return this.startTime;
	}
	
	public Date getEndTime(){
		return this.endTime;
	}
	
	//getters & setters
	public File getIndexDir() {
		return indexDir;
	}

	public void setIndexDir(File indexDir) {
		this.indexDir = indexDir;
	}

	public int getNumOfJobs() {
		return numOfJobs;
	}

	public void setNumOfJobs(int numOfJobs) {
		this.numOfJobs = numOfJobs;
	}
}
