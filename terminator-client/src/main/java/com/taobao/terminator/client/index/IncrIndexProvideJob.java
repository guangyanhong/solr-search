package com.taobao.terminator.client.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class IncrIndexProvideJob implements Job{
	public static final String INDEX_PROVIDER_NAME = "incrIndexProvider";
	protected static Log log = LogFactory.getLog(IndexProvider.class);
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		String serviceName = (String)context.getJobDetail().getJobDataMap().get("incrServiceName");
		log.warn("[Full-IndexProvide-Job] 运行增量任务 serviceName ==> " + serviceName);
		
		if(DumperController.getInstance(serviceName).localhostCanRun(false)){
			IndexProvider indexProvider = (IndexProvider)context.getJobDetail().getJobDataMap().get(INDEX_PROVIDER_NAME);
			indexProvider.dump();
		}else{
			log.warn("[FullIndexProvideJob] ===================> 当前机器不可运行增量任务，直接返回,可能有集群中其他机器运行该任务.");
		}
	}
}
