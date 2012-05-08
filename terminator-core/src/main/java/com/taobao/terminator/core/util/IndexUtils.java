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
		//首先要将incr_dump_start_time_重命名为incr_dump_start_time
		File file = new File(solrCore.getCoreDescriptor().getCoreContainer().getSolrHome()
				+ File.separator + solrCore.getName() 
				+ File.separator + "conf" + File.separator + TMP_INCR_DUMP_START_TIME_FILE_NAME);
		
		if(!file.exists()){
			logger.error("找不到incr_dump_start_time_文件，无法将其重命名。。。");
			throw new RuntimeException("找不到incr_dump_start_time_文件，全量索引失败。。。");
		}
		
		//删除原来的incr_dump_start_time
		File file2 = new File(solrCore.getCoreDescriptor().getCoreContainer().getSolrHome()
				+ File.separator + solrCore.getName() 
				+ File.separator + "conf" + File.separator + INCR_DUMP_START_TIME_FILE_NAME);
		
		if(file2.exists()){
			if(!file2.delete()){
				logger.error("无法删除incr_dump_start_time。");
				throw new RuntimeException("无法删除incr_dump_start_time。");
			}
		}
		
		if(!file.renameTo(file2)){
			logger.error("无法将incr_dump_start_time_重命名为incr_dump_start_time");
			throw new RuntimeException("无法将incr_dump_start_time_重命名为incr_dump_start_time");
		}
	}
	
	/**
	 * 将source solrcore的data dir指向dest solrcore的data dir，然后关闭dest solrcore
	 * @param oldCore
	 * 	当前的solrcore
	 * @param newCore
	 * 	目标solrcore
	 */
	public static SolrCore swapCores(SolrCore oldCore, SolrCore newCore){		
		logger.warn("当前solrCore的dataDir：" + oldCore.getDataDir());
		logger.warn("要交换到的dataDir：" + newCore.getDataDir());
		String coreName = oldCore.getName();
		String oldDataDir = oldCore.getDataDir();
		
		oldCore.getCoreDescriptor().setDataDir(newCore.getDataDir());
		
		//关闭dest
		while(!newCore.isClosed()){
			logger.warn("newCore关闭时间:" + new Date());
			newCore.close();
		}
		
		CoreContainer coreContainer = oldCore.getCoreDescriptor().getCoreContainer();
		try{
			logger.warn("coreContainer reload时间:" + new Date());
			coreContainer.reload(coreName);
		} catch (Exception e) {
			logger.error("重新打开SolrCore失败。。。。。", e);
			throw new RuntimeException("重新打开dest失败，solr core无法继续使用。solr core:" + oldCore.getName());
		}
		
		//先关闭src，然后reload
		while(!oldCore.isClosed()){
			logger.warn("oldCore关闭时间:" + new Date());
			oldCore.close();
		}
		
		if(oldCore.getCoreDescriptor().getCoreContainer().isPersistent()){
			oldCore.getCoreDescriptor().getCoreContainer().persist();
		}
		
		//删除原来的core使用的data目录下的索引数据
		try {
			logger.warn("清除旧索引文件时间:" + new Date());
			cleanDir(new File(oldDataDir));
		} catch (IOException e) {
			logger.error("清空索引数据目录失败。dataDir:" + oldDataDir, e);
		}
		
		
		return coreContainer.getCore(coreName);
	}
	
	/**
	 * 在开始一次全量索引之前，用来创建一个新的core，新的索引被导入到这个core里面
	 * 全量索引完成之后，要切换两个core
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
		
		//如果当前存在data-back目录，那么将该目录下的所有文件和目录清空，以便放置新的索引数据
		File temp = new File(dataDir);
		
		if(temp.exists()){
			cleanDir(temp);
		}
		
		//用现有的solr core创建一个新的core，并将data目录指向 data-back
		SolrCore core = new SolrCore(currentName, dataDir, srouce.getSolrConfig(), srouce.getSchema(), srouce.getCoreDescriptor());
		//设置新的solrCore的CoreDescriptor的data dir，保持与新的core的dataDir的同步
		core.getCoreDescriptor().setDataDir(dataDir);
		return core;
	}
	
	public static void cleanDir(File dataDir) throws IOException{
		//清除core的data目录下面的所有文件和目录
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
				logger.error("无法创建incr_dump_start_time_文件，全量索引失败。。。");
				throw new RuntimeException("无法创建incr_dump_start_time_文件，全量索引失败。。。");
			}
		}
		
		FileOutputStream fout = null;
		
		try {
			fout = new FileOutputStream(incrDumpStartTime);
			fout.write(String.valueOf(new Date().getTime()).getBytes());
		} catch (FileNotFoundException e) {
			logger.error("找不到incr_dump_start_time_文件，全量索引失败。。。", e);
			throw new RuntimeException("找不到incr_dump_start_time_文件，全量索引失败。。。");
		} catch (IOException e) {
			logger.error("写incr_dump_start_time_文件失败，全量索引失败。。。", e);
			throw new RuntimeException("写incr_dump_start_time_文件失败，全量索引失败。。。");
		} finally{
			if(fout != null){
				try {
					fout.close();
				} catch (IOException e) {
					logger.error("关闭incr_dump_start_time_文件对应的输出流失败。。。", e);
				}
			}
		}
	}
}
