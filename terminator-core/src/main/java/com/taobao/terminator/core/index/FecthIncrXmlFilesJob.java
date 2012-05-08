package com.taobao.terminator.core.index;

import org.apache.commons.logging.Log;
import org.apache.solr.core.SolrCore;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import com.taobao.terminator.core.service.inner.DefaultSlaveService.FetchIncrFilesJob;

public class FecthIncrXmlFilesJob implements Job{
	private SolrCore solrCore;
	public FecthIncrXmlFilesJob(){}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		Log logger = null;
		try {
			this.solrCore = (SolrCore)context.getScheduler().getContext().get("solrCore");
			logger = (Log)context.getScheduler().getContext().get(this.solrCore.getName() + "-logger");
			FetchIncrFilesJob fetchIncrFilesJob = (FetchIncrFilesJob)context.getScheduler().getContext().get(this.solrCore.getName() + "-fetchIncrFilesJob");
			logger.warn("从Master获取增量xml文件.....");
			fetchIncrFilesJob.fetch();
		} catch (SchedulerException e2) {
			if(logger != null){
				logger.error("从Master获取增量xml文件任务失败.",e2);
			}
		}
	}
}
