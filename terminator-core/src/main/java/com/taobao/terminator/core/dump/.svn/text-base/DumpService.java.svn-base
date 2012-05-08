package com.taobao.terminator.core.dump;

public interface DumpService {
	
	public final static String JOB_SOLR_CORE = "solrCore";
	public final static String JOB_NEW_SOLR_CORE = "newSolrCore";
	public final static String JOB_FULL_INDEX_PROVIDER = "FullIndexProvider";
	public final static String JOB_INCR_INDEX_PROVIDER = "IncrIndexProvider";
	public final static String JOB_CAN_EXECUTE = "jobCanExecute";
	public final static String FULL_INDEX_BEGIN_TIME = "fullIndexBeginTime";
	
	/**
	 * Master角色全量Dump完毕后通知Slave角色的机器，告诉Slave角色的机器来Master复制全量后的索引文件
	 * 
	 * @param masterIp
	 * @param fileNames
	 */
	public boolean notifyToSlave(String masterIp,int port,String[] fileNames,String incrTime);
	
	/**
	 * Slave角色的机器向Master角色的机器回报复制全量后索引的情况
	 * 
	 * @param slaveIp
	 * @param msg
	 */
	public boolean reportToMaster(String slaveIp,String msg);
}
