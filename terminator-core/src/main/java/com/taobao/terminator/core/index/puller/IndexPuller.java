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
 * �����ļ���ץȡ��
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
	
	private String           indexDir = null; //�����ļ���master�����ϵ�·��
	private List<String>     fileList = null; //��Ҫ���ص��ļ������б�
	private Map<String,Long> fileNameSizeMaps = null; //�ļ��� ==> ��С��ӳ��
	private List<PendingFile> pendingFileList = null;
	
	/* ȷ�����������ĵ���˳�� */
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
		log.warn(">>>Full-Copy-Index<<< ���������ļ�����...");
		
		log.warn("[step-1] ��ȡ�ļ����б�.");
		puller.fetchIndexFileNameList();
		
		log.warn("[step-2] ��master���������������ļ�.");
		puller.fetchIndexFiles();

		
		log.warn("[step-4] У���ļ����ص���ȷ��.");
		boolean isOk = puller.checksumFiles();
		if(!isOk){
			log.error(">>>Full-Copy-Index<<< ���������ļ����ƽ����������ļ����ش������⣬У�����.");
		}else{
			log.warn(">>>Full-Copy-Index<<< ���������ļ����ƽ��� ==>�ɹ���ɴ˴��ļ����Ʋ���,�ܺ�ʱ ��{" + (System.currentTimeMillis() - startTime)/1000 +" s}");
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
	 * ��ʼ��HttpClient����
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
			throw new IndexPullException("��������˳����ȷ.");
		}
	}
	
	/**
	 * ��ȡ���е������ļ���
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
			throw new IndexPullException("��ȡ�ļ��б�ʧ��",e);
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
			throw new IndexPullException("��ȡ�ļ� --> �ļ���Сӳ��ʧ��",e);
		}
		this.checkRspStatus(nl);
		this.fileNameSizeMaps = (Map<String,Long>)nl.get(IndexPullHandler.RESP_FILE_SIZE_MAPS);
	}
	
	/**
	 * ��ȡ�����������ļ�
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
				log.warn("�������Ŀ¼  ==> " + indexDir.getAbsolutePath());
				try {
					FileUtils.cleanDirectory(indexDir);
				} catch (IOException e) {
					throw new IndexPullException("�������Ŀ¼ ==>" + indexDir.getAbsolutePath() + " ʧ��.",e);
				}
			}
			
			for(String fileName : fileList){
				log.warn("��ȡ�����ļ� ��fileName ==> " + fileName);
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
						log.error("��ȡInputStreamʧ��,����pendingFileList��������",e);
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
							throw new IndexPullException("���������ļ�ʧ��  ==> " + file.getAbsoluteFile(),e);
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
						throw new IndexPullException("д�����ļ�ʧ��[Stream����] ==> " + file.getAbsolutePath(), e);
					} finally {
						if(inputStream != null)
							IOUtils.closeQuietly(inputStream);
						if(outputStream != null)
							IOUtils.closeQuietly(outputStream);
					}
					log.warn("��ȡ�����ļ� ��fileName ==> " + fileName +  " �ܺ�ʱ ��{" + (System.currentTimeMillis() - startTime)/1000 +" s}");
				}finally{
					post.releaseConnection(); //������ȡ��֮������ͷ�����
				}
			}
			if(pendingFileList != null && !pendingFileList.isEmpty()){
				log.error("��ȡ�����ļ����ɹ����б���ԭ��:\n");
				for(PendingFile pf : pendingFileList){
					log.error(pf.toString());
				}
			}
		}else{
			throw new IndexPullException("FileList��û�л�ȡ�����Ȼ�ȡFileList���ڻ�ȡ�����ļ�.");
		}
	}
	
	/**
	 * У�鴫���ļ�����ȷ��
	 * <ul><li>
	 * 1.����<li>
	 * 2.�ļ�����<li>
	 * 3.��Ӧ�ļ��Ĵ�С
	 * 
	 * @return
	 * @throws IOException
	 */
	public boolean checksumFiles() throws IndexPullException{
		this.checkStep(STEP_CHECKSUMFILE);
		currentStep ++;
		log.warn(">>checksumFiles<< ��֤�ļ��������ȷ��.");
		this.fetchIndexFileSizeMaps();
		
		File indexDir = this.getIndexDir();
		File[] localFiles = indexDir.listFiles();
		int mastersFileNum = this.fileNameSizeMaps.size();
		int localFileNum   = localFiles.length;
		
		if(localFileNum != mastersFileNum){ //�ļ���������
			log.error(">>checksumFiles<< �ļ���Ŀ����,localFileNum ==> " + localFileNum + "  mastresFileNum ==> " + mastersFileNum);
			return false;
		}
		
		for(File localFile : localFiles){
			String name = localFile.getName();
			if(this.fileNameSizeMaps.containsKey(name)){
				Long size = fileNameSizeMaps.get(name);
				Long localFileSize = localFile.length();
				if(!size.equals(localFileSize)){
					log.error(">>checksumFiles<< �ļ���С����,fileName ==> " + name + "localFileSize is [" + localFileSize +"]  mastersFileSize is [" + size + "]");
					return false;
				}
			}
		}
		log.warn(">>checksumFiles<< �ļ���֤�ɹ�!");
		return true;
	}
	
	
	/**
	 * ��ȡ���ص�Solr��������ŵ�Ŀ¼
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
			throw new IndexPullException("�����ļ�Ŀ¼Ӧ����Ŀ¼�ļ������Ƕ�Ӧ�ļ�·��ȴ��һ���ļ�������Ŀ¼ ==> " + indexDir.getAbsolutePath());
		}
		
		return indexDir;
	}
	
	/**
	 * ��֤�����Ƿ�ɹ������Ƿ���Master�˳����˴���
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
	 * �����ȡNamedList���͵���Ӧ
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
				log.error("Http����ʧ��  URL ==> " + method,e);
				throw e;
			}
			return (NamedList) new JavaBinCodec().unmarshal(method.getResponseBodyAsStream());
		} finally {
			try {
				method.releaseConnection();
			} catch (Exception e) {
				log.error("�ͷ�HttpClient��Http����ʧ��.");
			}
		}
	}
	
	/**
	 * �����ȡInputStream���͵���Ӧ
	 * 
	 * @param method
	 * @return
	 * @throws IOException
	 */
	private InputStream getInputStreamResponse(PostMethod method)throws IOException {
		int status = httpClient.executeMethod(method);
		if (status != HttpStatus.SC_OK) {
			RuntimeException e =  new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Request failed for the url " + method);
			log.error("Http����ʧ��  URL ==> " + method,e);
			throw e;
		}
		return method.getResponseBodyAsStream();
		
	}
	
	private static class PendingFile{
		public static String REASON_FETCH_INPUT_STREAM_ERROR = "ͨ��HTTPClient��ȡmaster�����ϵ�������";
		public static String REASON_CREATE_FILE_ON_LOCAL     = "���ش����ļ�ʧ��";
		public static String REASON_WRITE_FILE_ON_LOCAL      = "д�����ļ�ʧ��";
		
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
