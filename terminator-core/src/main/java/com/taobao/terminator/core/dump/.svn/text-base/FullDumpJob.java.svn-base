package com.taobao.terminator.core.dump;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.UpdateHandler;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import org.quartz.UnableToInterruptJobException;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.core.realtime.TerminatorUpdateHandler;
import com.taobao.terminator.core.util.IndexUtils;

/**
 * 全量Dump任务
 * 
 * @author yusen
 */
public class FullDumpJob implements InterruptableJob {
	
	private static Log logger = LogFactory.getLog(FullDumpJob.class);
	
	private FullIndexProvider indexProvider;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		String jobCanExecute = null;
		String coreName      = null;
		try {
			coreName = (String)context.getJobDetail().getJobDataMap().get("coreName");
			logger.warn("[Full-Index] 全量Dump任务开始,CoreName ==> " + coreName);
			jobCanExecute = (String)context.getJobDetail().getJobDataMap().get(DumpService.JOB_CAN_EXECUTE);
		} catch (Exception e) {
			throw new JobExecutionException("[Full-Index] 从ScheduleContext中获取参数失败",e);
		}
		
		if(!"yes".equals(jobCanExecute)) {
			throw new JobExecutionException("[Full-Index] 该机器此时不是Master角色,不进行全量Dump任务.");
		}
		
		SolrCore solrCore = null;
		indexProvider = null;
		try {
			solrCore = (SolrCore)context.getScheduler().getContext().get(DumpService.JOB_SOLR_CORE);
			indexProvider = (FullIndexProvider)context.getScheduler().getContext().get(DumpService.JOB_FULL_INDEX_PROVIDER);
		} catch (Exception e) {
			throw new JobExecutionException("[Full-Index] 从ScheduleContext中获取参数失败",e);
		}
		
		SolrCore newCore = null;
		try {
			newCore = IndexUtils.newSolrCore(solrCore);
			UpdateHandler updateHandler = newCore.getUpdateHandler();
			if(updateHandler instanceof TerminatorUpdateHandler) {
				((TerminatorUpdateHandler)updateHandler).switchMode(TerminatorUpdateHandler.MODE_DIRECT);
			}
		} catch (Exception e) {
			try {
				context.getScheduler().getContext().put("newSolrCore", solrCore);
			} catch (SchedulerException e1) {
				logger.error("[Full-Index] 存储新core位置失败", e1);
			}
			throw new JobExecutionException("[Full-Index] 创建新的SolrCore失败",e);
		}
		logger.warn("[Full-Index] 即将全量的索引的索引目录 ===> " + newCore.getDataDir());
		indexProvider.setSolrCore(newCore);
		
		//开启全量超时监听线程
		/*(new TimeoutThread(12 * 60 * 60 * 1000) {
			@Override
			public void handleTimeoutEvent() {
				logger.equals("全量任务超时");
				try {
					FullDumpJob.this.interrupt();
				} catch (UnableToInterruptJobException e) {
					logger.error("全量任务超时，在终止时发生异常", e);
				}
			}
		}).start();*/
		
		try {
			indexProvider.dump();
			context.getScheduler().getContext().put(DumpService.JOB_NEW_SOLR_CORE, newCore);
			context.getScheduler().getContext().put(DumpService.FULL_INDEX_BEGIN_TIME, TerminatorCommonUtils.formatDate(indexProvider.getStartTime()));
		} catch(DumpFatalException dfe) {
			logger.fatal("[Full-Index] dump数据时出现致命错误，本次全量dump失败", dfe);
			throw new JobExecutionException("[Full-Index] dump数据时出现致命错误，本次全量dump失败", dfe);
		} catch (SchedulerException e) {
			logger.error("存储新core位置失败", e);
		} 
		
		logger.warn("[Full-Index] 全量索引顺利结束，已经被写入新的SolrCore,索引目录 ===> " + newCore.getDataDir() + " 全量结束时间：" + new Date());
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		this.indexProvider.getInterrupted().set(true);
	}

}
