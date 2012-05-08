package com.taobao.terminator.client.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class FullIndexProvideJob implements Job{
	public static final String INDEX_PROVIDER_NAME = "fullIndexProvider";
	protected static Log log = LogFactory.getLog(IndexProvider.class);
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		String serviceName = (String)context.getJobDetail().getJobDataMap().get("fullServiceName");
		log.warn("[Full-IndexProvide-Job] 运行全量任务 serviceName ==> " + serviceName);
		
		if(DumperController.getInstance(serviceName).localhostCanRun(true)){
			IndexProvider indexProvider = (IndexProvider)context.getJobDetail().getJobDataMap().get(INDEX_PROVIDER_NAME);
			indexProvider.dump();
		}else{
			log.warn("[FullIndexProvideJob] ===================> 当前机器不可运行全量任务，直接返回,可能有集群中其他机器运行该任务.");
		}
	}
}
