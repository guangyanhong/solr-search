package com.taobao.terminator.core.index;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.SolrCore;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.taobao.terminator.core.index.importer.DataImporter;
import com.taobao.terminator.core.index.importer.FullDataImporter;
import com.taobao.terminator.core.util.IndexUtils;

public final class FullIndexWriteJob  implements Job  {
	protected static Log logger = LogFactory.getLog(FullIndexWriteJob.class);
	
	public void execute(JobExecutionContext jobContext) throws JobExecutionException {		
		logger.warn("[Full-Index] 全量索引消费开始。。。。");
		
		SolrCore solrCore = null;
		IndexFileSearcher fileSearcher = null;
		try {
			solrCore = (SolrCore)jobContext.getScheduler().getContext().get("solrCore");
			fileSearcher = (IndexFileSearcher)jobContext.getScheduler().getContext().get(solrCore.getName() + "-fullIndexFileSearcher");
		} catch (Exception e) {
			throw new JobExecutionException("[Full-Index] 从ScheduleContext中获取参数失败",e);
		}
		
		SolrCore indexingCore = null;
		try {
			logger.warn("[Full-Index] 创建新的Core.");
			indexingCore = IndexUtils.newSolrCore(solrCore);
		} catch (Exception e) {
			jobContext.put("newSolrCore", solrCore);
			throw new JobExecutionException("[Full-Index] 创建新的SolrCore失败",e);
		} 
		
		logger.warn("[Full-Index] 即将全量的索引的索引目录 ===> " + indexingCore.getDataDir());
		
		try{
			DataImporter dataImprtor = new FullDataImporter(indexingCore,fileSearcher);
			dataImprtor.importData();
			jobContext.put("newSolrCore", indexingCore);
		} catch (Exception e){
			throw new JobExecutionException("[Full-Index] 全量Dump时异常",e);
		}
		
		logger.warn("[Full-Index] 全量索引顺利结束，已经被写入新的SolrCore,索引目录 ===> " + indexingCore.getDataDir() + " 全量结束时间：" + new Date());
	}
}
