package com.taobao.terminator.core.dump;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.SolrCore;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

/**
 * 增量Dump任务
 * 
 * @author yusen
 */
public class IncrDumpJob implements InterruptableJob {
	
	private static Log logger = LogFactory.getLog(IncrDumpJob.class);
	
	private IncrIndexProvider incrIndexProvider;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		logger.warn("**********增量时间任务开始**********");
		incrIndexProvider = null;
		SolrCore solrCore = null;
		try {
			incrIndexProvider = (IncrIndexProvider)context.getScheduler().getContext().get(DumpService.JOB_INCR_INDEX_PROVIDER);
			solrCore = (SolrCore)context.getScheduler().getContext().get(DumpService.JOB_SOLR_CORE);
		} catch(Exception e) {
			throw new JobExecutionException("[Incr-Index] 从ScheduleContext中获取参数失败",e);
		}
		
		if(incrIndexProvider == null || solrCore == null) {
			throw new JobExecutionException("[Incr-Index] 获得IndexProvider或SolrCore失败");
		}
		
		try {
			incrIndexProvider.setSolrCore(solrCore);
			incrIndexProvider.dump();
		} catch(DumpFatalException dfe) {
			logger.fatal("[Incr-Index] dump数据时出现致命错误，本次增量dump失败", dfe);
			throw new JobExecutionException("[Incr-Index] dump数据时出现致命错误，本次增量dump失败", dfe);
		} 
		
		logger.warn("[Incr-Index] 增量任务顺利结束，" + " 增量结束时间：" + new Date());
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		this.incrIndexProvider.getInterrupted().set(true);
	}
}
