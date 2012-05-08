package com.taobao.terminator.core.index.puller;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 索引文件的抓取器
 * 
 * @author yusen
 */
public class IndexPuller {
	
	protected static Logger log = LoggerFactory.getLogger(IndexPuller.class);
	
	public static final String MASTER_URL = "masterUrl";
	public static final String HTTP_CONN_TIMEOUT = "httpConnTimeout";
	public static final String HTTP_READ_TIMEOUT = "httpReadTimeout";
		
	private HttpClient httpClient = null;
	private String     masterUrl  = null;
	
	private int readTimeout = 20 * 1000;
	private int connTimeout = 5 * 1000;
	
	private String           indexDir = null; //索引文件在master机器上的路径
	private List<String>     fileList = null; //需要下载的文件名称列表
	private Map<String,Long> fileNameSizeMaps = null; //文件名 ==> 大小的映射
	private List<PendingFile> pendingFileList = null;
	
	/* 确保几个方法的调用顺序 */
	private static final int STEP_FETCHLIST       = 1;
	private static final int STEP_FECHTFILE       = 2;
	private static final int STEP_CHECKSUMFILE    = 3;
	private int currentStep = 1;

	private SolrCore solrCore = null;
	
	public static void main(String[] args) throws Exception {
		NamedList<String> nl = new NamedList<String>();
		nl.add(MASTER_URL, "http://192.168.211.36:7001/terminator-web-2.5.0/search4album-0/indexPull");
		nl.add(HTTP_READ_TIMEOUT, "50000");
		nl.add(HTTP_CONN_TIMEOUT, "3000");
		
		IndexPuller puller = new IndexPuller(nl,null);
		long startTime = System.currentTimeMillis();
		log.warn(">>>Full-Copy-Index<<< 请求索引文件复制...");
		
		log.warn("[step-1] 获取文件名列表.");
		puller.fetchIndexFileNameList();
		
		log.warn("[step-2] 从master机器上下载索引文件.");
		puller.fetchIndexFiles();

		
		log.warn("[step-4] 校验文件下载的正确性.");
		boolean isOk = puller.checksumFiles();
		if(!isOk){
			log.error(">>>Full-Copy-Index<<< 请求索引文件复制结束，但是文件下载存在问题，校验出错.");
		}else{
			log.warn(">>>Full-Copy-Index<<< 请求索引文件复制结束 ==>成功完成此次文件复制操作,总耗时 ：{" + (System.currentTimeMillis() - startTime)/1000 +" s}");
		}
	}
	
	@SuppressWarnings("unchecked")
	public IndexPuller(NamedList initArgs,SolrCore solrCore){
		this.solrCore = solrCore;
		this.masterUrl = (String)initArgs.get(MASTER_URL);
		String readTimeoutStr = (String)initArgs.get(HTTP_READ_TIMEOUT);
		String connTimeoutStr = (String)initArgs.get(HTTP_CONN_TIMEOUT);
		if(readTimeoutStr != null){
			this.readTimeout = Integer.valueOf(readTimeoutStr);
		}
		if(connTimeoutStr != null){
			this.connTimeout = Integer.valueOf(connTimeoutStr);
		}
		this.initHttpClient();
	}
	
	/**
	 * 初始化HttpClient对象
	 * 
	 */
	private void initHttpClient(){
		MultiThreadedHttpConnectionManager mgr = new MultiThreadedHttpConnectionManager();
	    mgr.getParams().setDefaultMaxConnectionsPerHost(10000);
	    mgr.getParams().setMaxTotalConnections(10000);
	    mgr.getParams().setSoTimeout(readTimeout); 
	    mgr.getParams().setConnectionTimeout(connTimeout); 
	    this.httpClient = new HttpClient(mgr);
	}
	
	private void checkStep(int mystep)throws IndexPullException{
		if(currentStep != mystep){
			throw new IndexPullException("方法调用顺序不正确.");
		}
	}
	
	/**
	 * 获取所有的索引文件名
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public void  fetchIndexFileNameList() throws IndexPullException{
		checkStep(STEP_FETCHLIST);
		currentStep++;
		PostMethod post = new PostMethod(masterUrl);
		post.addParameter(IndexPullHandler.CMD, IndexPullHandler.CMD_GETFILELIST);
		post.addParameter("wt", "javabin");
		NamedList nl = null;
		try {
			nl = this.getNamedListResponse(post);
		} catch (IOException e) {
			throw new IndexPullException("获取文件列表失败",e);
		}
		
		this.checkRspStatus(nl);
		this.fileList = (List<String>)nl.get(IndexPullHandler.RESP_FILE_LIST);
		this.indexDir = (String)nl.get(IndexPullHandler.ARG_INDEX_DIR);
	}
	
	@SuppressWarnings("unchecked")
	private void fetchIndexFileSizeMaps() throws IndexPullException{
		PostMethod post = new PostMethod(masterUrl);
		post.addParameter(IndexPullHandler.CMD, IndexPullHandler.CMD_GETFILESIZEMAPS);
		post.addParameter("wt", "javabin");
		NamedList nl = null;
		try {
			nl = this.getNamedListResponse(post);
		} catch (IOException e) {
			throw new IndexPullException("获取文件 --> 文件大小映射失败",e);
		}
		this.checkRspStatus(nl);
		this.fileNameSizeMaps = (Map<String,Long>)nl.get(IndexPullHandler.RESP_FILE_SIZE_MAPS);
	}
	
	/**
	 * 获取并保存索引文件
	 * 
	 * @throws IOException
	 */
	public void fetchIndexFiles() throws  IndexPullException{
		checkStep(STEP_FECHTFILE);
		currentStep ++;
		if(fileList != null && !fileList.isEmpty()){
			File indexDir = this.getIndexDir();
			File[] allFiles = indexDir.listFiles();
			if(allFiles != null && allFiles.length > 0){
				log.warn("清空索引目录  ==> " + indexDir.getAbsolutePath());
				try {
					FileUtils.cleanDirectory(indexDir);
				} catch (IOException e) {
					throw new IndexPullException("清空索引目录 ==>" + indexDir.getAbsolutePath() + " 失败.",e);
				}
			}
			
			for(String fileName : fileList){
				log.warn("获取索引文件 ，fileName ==> " + fileName);
				long startTime = System.currentTimeMillis();
				PostMethod post = new PostMethod(masterUrl);
				post.addParameter(IndexPullHandler.CMD, IndexPullHandler.CMD_GETFILE);
				post.addParameter(IndexPullHandler.ARG_FILENAME, fileName);
				post.addParameter(IndexPullHandler.ARG_INDEX_DIR, this.indexDir);
				post.addParameter("wt", IndexPullHandler.RESP_FILE_STREAM);
				
				try{
					InputStream inputStream = null;
					try {
						inputStream = this.getInputStreamResponse(post);
					} catch (IOException e) {
						log.error("获取InputStream失败,计入pendingFileList，继续！",e);
						if(pendingFileList == null){
							pendingFileList = new ArrayList<PendingFile>();
						}
						pendingFileList.add(new PendingFile(fileName, PendingFile.REASON_FETCH_INPUT_STREAM_ERROR));
						continue;
					}
					
					File file = new File(indexDir,fileName);
					if(!file.exists()){
						try {
							file.createNewFile();
						} catch (IOException e) {
							if(pendingFileList == null){
								pendingFileList = new ArrayList<PendingFile>();
							}
							pendingFileList.add(new PendingFile(fileName, PendingFile.REASON_CREATE_FILE_ON_LOCAL));
							throw new IndexPullException("创建索引文件失败  ==> " + file.getAbsoluteFile(),e);
						}
					}
					
					FileOutputStream outputStream = null;
					try {
						outputStream = new FileOutputStream(file);
						IOUtils.copy(inputStream, outputStream);
					} catch (Exception e) {
						if(pendingFileList == null){
							pendingFileList = new ArrayList<PendingFile>();
						}
						pendingFileList.add(new PendingFile(fileName, PendingFile.REASON_WRITE_FILE_ON_LOCAL));
						throw new IndexPullException("写索引文件失败[Stream复制] ==> " + file.getAbsolutePath(), e);
					} finally {
						if(inputStream != null)
							IOUtils.closeQuietly(inputStream);
						if(outputStream != null)
							IOUtils.closeQuietly(outputStream);
					}
					log.warn("获取索引文件 ，fileName ==> " + fileName +  " 总耗时 ：{" + (System.currentTimeMillis() - startTime)/1000 +" s}");
				}finally{
					post.releaseConnection(); //输入流取完之后才能释放连接
				}
			}
			if(pendingFileList != null && !pendingFileList.isEmpty()){
				log.error("获取索引文件不成功的列表及其原因:\n");
				for(PendingFile pf : pendingFileList){
					log.error(pf.toString());
				}
			}
		}else{
			throw new IndexPullException("FileList还没有获取，请先获取FileList后在获取索引文件.");
		}
	}
	
	/**
	 * 校验传输文件的正确性
	 * <ul><li>
	 * 1.数量<li>
	 * 2.文件名称<li>
	 * 3.对应文件的大小
	 * 
	 * @return
	 * @throws IOException
	 */
	public boolean checksumFiles() throws IndexPullException{
		this.checkStep(STEP_CHECKSUMFILE);
		currentStep ++;
		log.warn(">>checksumFiles<< 验证文件传输的正确性.");
		this.fetchIndexFileSizeMaps();
		
		File indexDir = this.getIndexDir();
		File[] localFiles = indexDir.listFiles();
		int mastersFileNum = this.fileNameSizeMaps.size();
		int localFileNum   = localFiles.length;
		
		if(localFileNum != mastersFileNum){ //文件数量不等
			log.error(">>checksumFiles<< 文件数目不等,localFileNum ==> " + localFileNum + "  mastresFileNum ==> " + mastersFileNum);
			return false;
		}
		
		for(File localFile : localFiles){
			String name = localFile.getName();
			if(this.fileNameSizeMaps.containsKey(name)){
				Long size = fileNameSizeMaps.get(name);
				Long localFileSize = localFile.length();
				if(!size.equals(localFileSize)){
					log.error(">>checksumFiles<< 文件大小不符,fileName ==> " + name + "localFileSize is [" + localFileSize +"]  mastersFileSize is [" + size + "]");
					return false;
				}
			}
		}
		log.warn(">>checksumFiles<< 文件验证成功!");
		return true;
	}
	
	
	/**
	 * 获取本地的Solr的索引存放的目录
	 * 
	 * @return
	 * @throws IOException
	 */
	private File getIndexDir() throws IndexPullException{
		String dataDir = "D:\\data-puller";//this.solrCore.getDataDir();
		File indexDir = new File(dataDir,"index");
		if(!indexDir.exists()){
			indexDir.mkdirs();
		}
		if(indexDir.isFile()){
			throw new IndexPullException("索引文件目录应该是目录文件，但是对应文件路径却是一个文件，而非目录 ==> " + indexDir.getAbsolutePath());
		}
		
		return indexDir;
	}
	
	/**
	 * 验证请求是否成功处理，是否在Master端出现了错误
	 * 
	 * @param nl
	 * @throws IndexPullException
	 */
	@SuppressWarnings("unchecked")
	private void checkRspStatus(NamedList nl) throws IndexPullException{
		String statusStr = (String)nl.get(IndexPullHandler.RESP_STATUS);
		if(statusStr.equals(IndexPullHandler.RESP_STATUS_ERROR)){
			String errorMsg = (String)nl.get(IndexPullHandler.RESP_ERRO_MSG);
			throw new IndexPullException(errorMsg);
		}
	}
	
	/**
	 * 请求获取NamedList类型的响应
	 * 
	 * @param method
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private NamedList getNamedListResponse(PostMethod method) throws IOException {
		try {
			int status = httpClient.executeMethod(method);
			if (status != HttpStatus.SC_OK) {
				RuntimeException e =  new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Request failed for the url " + method);
				log.error("Http请求失败  URL ==> " + method,e);
				throw e;
			}
			return (NamedList) new JavaBinCodec().unmarshal(method.getResponseBodyAsStream());
		} finally {
			try {
				method.releaseConnection();
			} catch (Exception e) {
				log.error("释放HttpClient的Http连接失败.");
			}
		}
	}
	
	/**
	 * 请求获取InputStream类型的响应
	 * 
	 * @param method
	 * @return
	 * @throws IOException
	 */
	private InputStream getInputStreamResponse(PostMethod method)throws IOException {
		int status = httpClient.executeMethod(method);
		if (status != HttpStatus.SC_OK) {
			RuntimeException e =  new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Request failed for the url " + method);
			log.error("Http请求失败  URL ==> " + method,e);
			throw e;
		}
		return method.getResponseBodyAsStream();
		
	}
	
	private static class PendingFile{
		public static String REASON_FETCH_INPUT_STREAM_ERROR = "通过HTTPClient获取master机器上的流错误";
		public static String REASON_CREATE_FILE_ON_LOCAL     = "本地创建文件失败";
		public static String REASON_WRITE_FILE_ON_LOCAL      = "写本地文件失败";
		
		public String fileName;
		public String reason;
		public PendingFile(String fileName, String reason) {
			super();
			this.fileName = fileName;
			this.reason = reason;
		}
		
		public String toString(){
			return "fileName ==> " + fileName + "  reason ==> " + reason;
		}
	}
}
