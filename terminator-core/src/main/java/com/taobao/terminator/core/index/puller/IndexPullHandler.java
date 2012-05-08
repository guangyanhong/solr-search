package com.taobao.terminator.core.index.puller;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.BinaryQueryResponseWriter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexPullHandler extends RequestHandlerBase implements SolrCoreAware {
	protected static Logger log = LoggerFactory.getLogger(IndexPullHandler.class);
	private SolrCore solrCore = null;

	public static final String CMD               = "cmd";
	public static final String CMD_GETFILELIST   = "getFileList";
	public static final String CMD_GETFILESIZEMAPS  = "getFileSizeMaps";
	public static final String CMD_GETFILE       = "getFile";
	public static final String CMD_COPYFROMMASTER= "copyFromMaster";
	
	public static final String ARG_FILENAME      = "fileName";
	public static final String ARG_INDEX_DIR     = "indexDir";
	
	public static final String RESP_FILE_STREAM  = "filestream";
	public static final String RESP_FILE_LIST    = "fileList";
	public static final String RESP_FILE_SIZE_MAPS = "fileSizeMaps";
	
	public static final String RESP_STATUS       = "status";
	public static final String RESP_STATUS_OK    = "0";
	public static final String RESP_STATUS_ERROR = "-1";
	public static final String RESP_ERRO_MSG     = "error_msg";
	

	@SuppressWarnings("unchecked")
	@Override
	public void init(NamedList args) {
		super.init(args);
	}
	
	@Override
	public void inform(SolrCore core) {
		this.solrCore = core;
		this.registerFileStreamResponseWriter();
	}

	@Override
	public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
		SolrParams solrParams = req.getParams();
		String cmd = solrParams.get(CMD);
		log.warn(">>>Full-Copy-Index<<< ȫ������Master�������ļ� ==> " + cmd);
		
		if(cmd.equals(CMD_COPYFROMMASTER)){
			long startTime = System.currentTimeMillis();
			log.warn(">>>Full-Copy-Index<<< ���������ļ�����...");
			IndexPuller puller = new IndexPuller(initArgs, solrCore);
			
			log.warn("[step-1] ��ȡ�ļ����б�.");
			puller.fetchIndexFileNameList();
			
			log.warn("[step-2] ��master���������������ļ�.");
			puller.fetchIndexFiles();

			log.warn("[step-3] У���ļ����ص���ȷ��.");
			boolean isOk = puller.checksumFiles();
			if(!isOk){
				log.error(">>>Full-Copy-Index<<< ���������ļ����ƽ����������ļ����ش������⣬У�����.");
			}else{
				log.warn(">>>Full-Copy-Index<<< ���������ļ����ƽ��� ==>�ɹ���ɴ˴��ļ����Ʋ���,�ܺ�ʱ ��{" + (System.currentTimeMillis() - startTime)/1000 +" s}");
				rsp.add(RESP_STATUS, RESP_STATUS_OK);
			}
		}else if (cmd.equals(CMD_GETFILELIST)) {
			log.warn(">>>Full-Copy-Index<<< ��ȡ�ļ����б�...");
			File indexDir = this.getIndexDir(req, rsp);
			if(indexDir == null) 
				return;
			
			File[] files = indexDir.listFiles(new FileFilter() {
				public boolean accept(File file) {
					return file.isFile();
				}
			});
			
			if(files == null || files.length == 0){
				rsp.add(RESP_STATUS, RESP_STATUS_ERROR);
				rsp.add(RESP_ERRO_MSG, "��Ӧ�������ļ���Ŀ¼��û�������ļ��������ļ�����Ϊ0");
				return;
			}
			
			log.warn(">>>Full-Copy-Index<<< �ļ����б����£�\n");
			List<String> fileNameList = new ArrayList<String>(files.length);
			
			for(File file :files){
				String name = file.getName();
				fileNameList.add(name);
				log.warn("FileName ==> " + name);
			}
			
			rsp.add(RESP_STATUS, RESP_STATUS_OK);
			rsp.add(RESP_FILE_LIST, fileNameList);
			rsp.add(ARG_INDEX_DIR, indexDir.getAbsolutePath());
			
		} else if (cmd.equals(CMD_GETFILE)) {
			String fileName = solrParams.get(ARG_FILENAME);
			String indexDir = solrParams.get(ARG_INDEX_DIR);
			File file = new File(indexDir,fileName);
			log.warn(">>>Full-Copy-Index<<< ��ȡ�ļ� file ==> " + file.getAbsolutePath());
			
			InputStream inputStream = null;
			try{
				inputStream = new FileInputStream(file);
			}catch(FileNotFoundException e){
				log.error(">>>Full-Copy-Index<<< ��ȡ�ļ� file ==> " + file.getAbsolutePath() +" ʧ��",e);
				inputStream = null; //������Ļ� ֱ�ӽ�null����
			}
		    
			rsp.add(RESP_FILE_STREAM, inputStream);
		    
		}else if(cmd.equals(CMD_GETFILESIZEMAPS)){
			log.warn(">>>Full-Copy-Index<<< ��ȡ�ļ��� --> �ļ���Сӳ���.");
			File indexDir = this.getIndexDir(req, rsp);
			if(indexDir == null) 
				return;
			
			File[] files = indexDir.listFiles(new FileFilter() {
				public boolean accept(File file) {
					return file.isFile();
				}
			});
			
			if(files == null || files.length == 0){
				rsp.add(RESP_STATUS, RESP_STATUS_ERROR);
				rsp.add(RESP_ERRO_MSG, "��Ӧ�������ļ���Ŀ¼��û�������ļ��������ļ�����Ϊ0");
				return;
			}
			
			Map<String,Long> fileNameSizeMaps = new HashMap<String,Long>(files.length);
			for(File file : files){
				fileNameSizeMaps.put(file.getName(), file.length());
			}
			rsp.add(RESP_STATUS, RESP_STATUS_OK);
			rsp.add(RESP_FILE_SIZE_MAPS, fileNameSizeMaps);
		}
	}
	
	/**
	 * ע���ļ�����ResponseWriter
	 */
	private void registerFileStreamResponseWriter(){
		solrCore.registerResponseWriter(RESP_FILE_STREAM, new BinaryQueryResponseWriter(){
			@Override
			public void write(OutputStream out, SolrQueryRequest request, SolrQueryResponse response) throws IOException {
				InputStream input = (InputStream)response.getValues().get(RESP_FILE_STREAM);
				if(input == null) return;
				try{
					byte[] buffer = new byte[1024 * 4];
					int n = 0;
					while (-1 != (n = input.read(buffer))) {
						out.write(buffer, 0, n);
					}
				}finally{
					IOUtils.closeQuietly(input);
				}
			}

			@Override
			public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
				return  "application/octet-stream";
			}

			@SuppressWarnings("unchecked")
			@Override
			public void init(NamedList args) {}

			@Override
			public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response) throws IOException {
				throw new RuntimeException("This is a binary writer , Cannot write to a characterstream");
			}
		});
	}
	
	/**
	 * ��ȡ��Ҫ���Ƶ������ļ�Ŀ¼
	 * 
	 * @param req
	 * @param rsp
	 * @return
	 */
	private File getIndexDir(SolrQueryRequest req, SolrQueryResponse rsp){
		SolrResourceLoader resourceLoader = solrCore.getResourceLoader();

		String dataDirStr = resourceLoader.getDataDir();
		String instanceDirStr = resourceLoader.getInstanceDir();

		File instanceDir = new File(instanceDirStr);
		if(!instanceDir.exists() || instanceDir.isFile()){
			rsp.add(RESP_STATUS, RESP_STATUS_ERROR);
			rsp.add(RESP_ERRO_MSG, "instanceDir ==> " + instanceDirStr +" ������ ���߲���Ŀ¼�ļ�.");
			return null;
		}
		
		File dataDir = new File(dataDirStr);
		
		if(!dataDir.exists() || dataDir.isFile()){
			rsp.add(RESP_STATUS, RESP_STATUS_ERROR);
			rsp.add(RESP_ERRO_MSG, "dataDir ==> " + dataDirStr +" ������ ���߲���Ŀ¼�ļ�.");
			return null;
		}
		
		String currentDataDirName = dataDir.getName();
		log.warn(">>>Full-Copy-Index<<< ��ǰCore����ʹ�õ�dataĿ¼�� ==> " + dataDirStr);
		
		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith("data");
			}
		};

		File[] dataDirs = instanceDir.listFiles(filter);
		if(dataDirs == null || dataDirs.length == 0){
			rsp.add(RESP_STATUS, RESP_STATUS_ERROR);
			rsp.add(RESP_ERRO_MSG, "instanceDir ==> " + instanceDirStr +" ������ [data*] ��ʽ����������Ŀ¼.");
			return null;
		}
		
		File bakDataDir = null; //�µ�core��dataĿ¼
		for(File f : dataDirs){
			String name = f.getName();
			if(!name.equals(currentDataDirName) && f.isDirectory()){
				bakDataDir = f;
				break;
			}
		}
		
		if(bakDataDir == null){
			rsp.add(RESP_STATUS, RESP_STATUS_ERROR);
			rsp.add(RESP_ERRO_MSG, "������bak��dataĿ¼.");
			return null;
		}
		
		log.warn(">>>Full-Copy-Index<<< ��ȫ����ϻ��ڵȴ��л��ɿɹ�ʹ�õ�core��dataĿ¼�� ==> " + bakDataDir.getAbsolutePath());
		File  indexDir = new File(bakDataDir,"index");
		return indexDir;
	}

	@Override
	public String getDescription() {
		return "Copy index-files From Master.";
	}

	@Override
	public String getSource() {
		return "no-source";
	}

	@Override
	public String getSourceId() {
		return "no-sourceId";
	}

	@Override
	public String getVersion() {
		return "1.0.0-by-YuSen";
	}
}
