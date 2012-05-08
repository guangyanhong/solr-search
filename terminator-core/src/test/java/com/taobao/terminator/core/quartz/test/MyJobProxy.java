package com.taobao.terminator.core.quartz.test;

import java.util.Date;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleTrigger;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;

public class MyJobProxy implements Job{
	private MyJob myJob = null;
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
		this.myJob = (MyJob)jobDataMap.get("myJob");
		this.myJob.execute();
	}
	
	public static void main(String[] args) throws Exception {
		SchedulerFactory sf = new StdSchedulerFactory();
		Scheduler sc = sf.getScheduler();
		JobDetail jobDetail = new JobDetail("job1","group1",MyJobProxy.class);
		jobDetail.getJobDataMap().put("myJob", new MyJob());
		
		SimpleTrigger trigger = new SimpleTrigger("trigger1","group1",new Date(TriggerUtils.getNextGivenSecondDate(null,1).getTime()));
		sc.scheduleJob(jobDetail,trigger);
		sc.start();
		Thread.sleep(5000);
		sc.shutdown();
	}
}
