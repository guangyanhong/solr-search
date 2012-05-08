package com.taobao.terminator.core.index;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.SolrCore;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

import com.taobao.terminator.core.util.IndexFileUtils;

public class OldIncrXmlRemoveJob implements Job {
	private static Log logger = LogFactory.getLog(OldIncrXmlRemoveJob.class);
	private SolrCore solrCore;
	
	public void execute(JobExecutionContext jobContext) throws JobExecutionException {
		String incrXmlRootDir = "";
		try {
			this.solrCore = (SolrCore)jobContext.getScheduler().getContext().get("solrCore");
			incrXmlRootDir = (String)jobContext.getScheduler().getContext().get(this.solrCore.getName() + "-incrXmlRootDir");
		} catch (SchedulerException e) {
			logger.error("获取存放增量xml文件的根目录失败。",e);
			return;
		}
		
		IndexFileUtils.cleanUpOldIncrXmlFile(new File(incrXmlRootDir));
	}

}
