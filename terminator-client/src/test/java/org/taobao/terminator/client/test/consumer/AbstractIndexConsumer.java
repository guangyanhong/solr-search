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
	 *	��¼û��д�ɹ�������������
	 */
	protected Map<String, File> pendingFiles;
	/**
	 * �����ļ�Ŀ¼
	 */
	protected File indexDir;
	/**
	 * �����첽д���������ļ����̳߳�
	 */
	protected ThreadPoolExecutor threadPool;
	/**
	 * �̳߳ص�������
	 */
	protected int numOfJobs;
	/**
	 * ��¼����������ʼ������ʱ��
	 */
	protected Date startTime;
	/**
	 * ��¼���������Ľ���ʱ��
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
					logger.error("��ȡ���������ļ���ʧ�ܣ����Ա����������ݡ�����", e);
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
				logger.error("���ļ�ʧ��.file:" + tmpIndexFile.getAbsolutePath(), e);
				throw new IllegalArgumentException("Can't open file. file:" + tmpIndexFile.getAbsolutePath());
			} catch (UnsupportedEncodingException e) {
				logger.error("JAVA�汾��֧�ָ��ַ���:UTF-8", e);
				throw new IllegalArgumentException("Unsupported Encoding:UTF-8");
			}
			
			try {
				//���һ����Χ��START_ELEMENT������xml����ʱ������
				out.write("<docs>");
				out.write(new String(this.data));
				//���һ����Χ��END_ELEMENT������xml����ʱ������
				out.write("</docs>");
			} catch (IOException e) {
				logger.error("����������д���ļ�ʱ����", e);
				AbstractIndexConsumer.this.addPending(fileName, tmpIndexFile);
				return;
			} finally{
				try {
					out.close();
				} catch (IOException e) {
					logger.error("�ر��ļ�ʧ��,file:" + tmpIndexFile.getAbsolutePath(), e);
				}
			}
			
			if(!tmpIndexFile.renameTo(new File(this.rename(fileName)))){
				logger.error("���ļ�" + tmpIndexFile.getAbsolutePath() + 
						"������Ϊ" + "" + "ʧ�ܣ�");
				AbstractIndexConsumer.this.addPending(fileName, tmpIndexFile);
			} 
		}
	}	
	
	/**
	 * ��ȡҪд�������xml�ļ��ľ���·��
	 * @param suffix
	 * @return
	 * @throws Exception
	 */
	protected abstract String getFilePath(String fileSuffix) throws Exception;
	
	protected synchronized void addPending(String fileName, File file){
		this.pendingFiles.put(fileName, file);
	}
	
	public void consum(byte[] data,String filePath) {
		logger.debug("���յ�����ԭʼ���ݣ�����buffer");
		//������㹻�Ŀռ��������µ�data����ô��ֱ�ӷ���buffer
		if(this.buffer.remaining() >= data.length){
			this.buffer.put(data);
		} else{//����buffer�е�����д���ļ���Ȼ��data����buffer
			logger.debug("buffer���Ѿ����㹻�����ݣ�д�뻺�档");
			this.buffer.flip();
			byte[] tmp = new byte[this.buffer.limit()];
			this.buffer.get(tmp);
			this.buffer.clear();
			this.buffer.put(data);
			this.threadPool.execute(new WriterIndexJob(tmp,filePath));
		}
		
		
		//new WriterIndexJob(data,filePath).run();
		logger.debug("��������ɡ�����");
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
