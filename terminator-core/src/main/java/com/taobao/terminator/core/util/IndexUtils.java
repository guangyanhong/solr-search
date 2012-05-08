package com.taobao.terminator.core.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.xml.sax.SAXException;

public final class IndexUtils {
	private static Log logger = LogFactory.getLog(IndexUtils.class);
	private final static String INCR_DUMP_START_TIME_FILE_NAME = "incr_dump_start_time";
	private final static String TMP_INCR_DUMP_START_TIME_FILE_NAME = "incr_dump_start_time_";
	
	public static void renameIncrStartTimeTMPFile(SolrCore solrCore){
		//����Ҫ��incr_dump_start_time_������Ϊincr_dump_start_time
		File file = new File(solrCore.getCoreDescriptor().getCoreContainer().getSolrHome()
				+ File.separator + solrCore.getName() 
				+ File.separator + "conf" + File.separator + TMP_INCR_DUMP_START_TIME_FILE_NAME);
		
		if(!file.exists()){
			logger.error("�Ҳ���incr_dump_start_time_�ļ����޷�����������������");
			throw new RuntimeException("�Ҳ���incr_dump_start_time_�ļ���ȫ������ʧ�ܡ�����");
		}
		
		//ɾ��ԭ����incr_dump_start_time
		File file2 = new File(solrCore.getCoreDescriptor().getCoreContainer().getSolrHome()
				+ File.separator + solrCore.getName() 
				+ File.separator + "conf" + File.separator + INCR_DUMP_START_TIME_FILE_NAME);
		
		if(file2.exists()){
			if(!file2.delete()){
				logger.error("�޷�ɾ��incr_dump_start_time��");
				throw new RuntimeException("�޷�ɾ��incr_dump_start_time��");
			}
		}
		
		if(!file.renameTo(file2)){
			logger.error("�޷���incr_dump_start_time_������Ϊincr_dump_start_time");
			throw new RuntimeException("�޷���incr_dump_start_time_������Ϊincr_dump_start_time");
		}
	}
	
	/**
	 * ��source solrcore��data dirָ��dest solrcore��data dir��Ȼ��ر�dest solrcore
	 * @param oldCore
	 * 	��ǰ��solrcore
	 * @param newCore
	 * 	Ŀ��solrcore
	 */
	public static SolrCore swapCores(SolrCore oldCore, SolrCore newCore){		
		logger.warn("��ǰsolrCore��dataDir��" + oldCore.getDataDir());
		logger.warn("Ҫ��������dataDir��" + newCore.getDataDir());
		String coreName = oldCore.getName();
		String oldDataDir = oldCore.getDataDir();
		
		oldCore.getCoreDescriptor().setDataDir(newCore.getDataDir());
		
		//�ر�dest
		while(!newCore.isClosed()){
			logger.warn("newCore�ر�ʱ��:" + new Date());
			newCore.close();
		}
		
		CoreContainer coreContainer = oldCore.getCoreDescriptor().getCoreContainer();
		try{
			logger.warn("coreContainer reloadʱ��:" + new Date());
			coreContainer.reload(coreName);
		} catch (Exception e) {
			logger.error("���´�SolrCoreʧ�ܡ���������", e);
			throw new RuntimeException("���´�destʧ�ܣ�solr core�޷�����ʹ�á�solr core:" + oldCore.getName());
		}
		
		//�ȹر�src��Ȼ��reload
		while(!oldCore.isClosed()){
			logger.warn("oldCore�ر�ʱ��:" + new Date());
			oldCore.close();
		}
		
		if(oldCore.getCoreDescriptor().getCoreContainer().isPersistent()){
			oldCore.getCoreDescriptor().getCoreContainer().persist();
		}
		
		//ɾ��ԭ����coreʹ�õ�dataĿ¼�µ���������
		try {
			logger.warn("����������ļ�ʱ��:" + new Date());
			cleanDir(new File(oldDataDir));
		} catch (IOException e) {
			logger.error("�����������Ŀ¼ʧ�ܡ�dataDir:" + oldDataDir, e);
		}
		
		
		return coreContainer.getCore(coreName);
	}
	
	/**
	 * �ڿ�ʼһ��ȫ������֮ǰ����������һ���µ�core���µ����������뵽���core����
	 * ȫ���������֮��Ҫ�л�����core
	 * @throws SAXException 
	 * @throws IOException 
	 * @throws ParserConfigurationException 
	 */
	public static SolrCore newSolrCore(SolrCore srouce) throws ParserConfigurationException, IOException, SAXException{
		String dataDir = srouce.getDataDir();
		String currentName = srouce.getName();
		
		int isBackDir = dataDir.lastIndexOf("-back");
		
		if(isBackDir >= 0){
			dataDir = dataDir.substring(0, isBackDir);
		} else{
			dataDir = dataDir.substring(0, dataDir.lastIndexOf(File.separator)) + "-back";
		}
		
		//�����ǰ����data-backĿ¼����ô����Ŀ¼�µ������ļ���Ŀ¼��գ��Ա�����µ���������
		File temp = new File(dataDir);
		
		if(temp.exists()){
			cleanDir(temp);
		}
		
		//�����е�solr core����һ���µ�core������dataĿ¼ָ�� data-back
		SolrCore core = new SolrCore(currentName, dataDir, srouce.getSolrConfig(), srouce.getSchema(), srouce.getCoreDescriptor());
		//�����µ�solrCore��CoreDescriptor��data dir���������µ�core��dataDir��ͬ��
		core.getCoreDescriptor().setDataDir(dataDir);
		return core;
	}
	
	public static void cleanDir(File dataDir) throws IOException{
		//���core��dataĿ¼����������ļ���Ŀ¼
		FileUtils.cleanDirectory(dataDir);
	}
	
	public static void createIncrStartTimeTMPFile(SolrCore solrCore){
		File incrDumpStartTime = new File(solrCore.getCoreDescriptor().getCoreContainer().getSolrHome()
				+ File.separator + solrCore.getName() 
				+ File.separator + "conf" + File.separator + TMP_INCR_DUMP_START_TIME_FILE_NAME);
		if(!incrDumpStartTime.exists()){
			try {
				incrDumpStartTime.createNewFile();
			} catch (IOException e) {
				logger.error("�޷�����incr_dump_start_time_�ļ���ȫ������ʧ�ܡ�����");
				throw new RuntimeException("�޷�����incr_dump_start_time_�ļ���ȫ������ʧ�ܡ�����");
			}
		}
		
		FileOutputStream fout = null;
		
		try {
			fout = new FileOutputStream(incrDumpStartTime);
			fout.write(String.valueOf(new Date().getTime()).getBytes());
		} catch (FileNotFoundException e) {
			logger.error("�Ҳ���incr_dump_start_time_�ļ���ȫ������ʧ�ܡ�����", e);
			throw new RuntimeException("�Ҳ���incr_dump_start_time_�ļ���ȫ������ʧ�ܡ�����");
		} catch (IOException e) {
			logger.error("дincr_dump_start_time_�ļ�ʧ�ܣ�ȫ������ʧ�ܡ�����", e);
			throw new RuntimeException("дincr_dump_start_time_�ļ�ʧ�ܣ�ȫ������ʧ�ܡ�����");
		} finally{
			if(fout != null){
				try {
					fout.close();
				} catch (IOException e) {
					logger.error("�ر�incr_dump_start_time_�ļ���Ӧ�������ʧ�ܡ�����", e);
				}
			}
		}
	}
}
