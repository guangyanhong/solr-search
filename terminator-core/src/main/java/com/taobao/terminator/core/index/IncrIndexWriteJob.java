package com.taobao.terminator.core.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.SolrCore;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.taobao.terminator.core.index.importer.DataImporter;
import com.taobao.terminator.core.index.importer.IncrDataImporter;

public final class IncrIndexWriteJob implements Job{
	protected static Log logger = LogFactory.getLog(FullIndexWriteJob.class);

	public void execute(JobExecutionContext jobContext) throws JobExecutionException {

		logger.warn("[Incr-Index] 增量索引任务开始.");
		if((Boolean)jobContext.get("isFullIndexing")){
			logger.warn("由于正在全量Dump索引，故此次增量任务被拒绝.");
			return;
		}
		
		SolrCore solrCore = null;
		IndexFileSearcher fileSearcher = null;
		try {
			solrCore = (SolrCore)jobContext.getScheduler().getContext().get("solrCore");
			fileSearcher = (IndexFileSearcher)jobContext.getScheduler().getContext().get(solrCore.getName() + "-incrIndexFileSearcher");
		} catch (Exception e) {
			throw new JobExecutionException("[Incr-Index] 从ScheduleContext中获取参数失败",e);
		} 
		
		try{
			DataImporter dataImporter = new IncrDataImporter(solrCore, fileSearcher);
			dataImporter.importData();
		}catch(Exception e){
			throw new JobExecutionException("[Incr-Index] 增量Dump时异常",e);
		}
	}
}
