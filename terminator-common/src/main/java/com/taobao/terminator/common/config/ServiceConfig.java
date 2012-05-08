package com.taobao.terminator.common.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.Watcher;

import com.taobao.terminator.common.TerminatorConstant;
import com.taobao.terminator.common.zk.TerminatorZKException;
import com.taobao.terminator.common.zk.TerminatorZKUtils;
import com.taobao.terminator.common.zk.TerminatorZkClient;

/**
 * 对应于一个搜索服务的集群节点配置
 * 
 * @author yusen
 */
public class ServiceConfig extends HashMap<String, GroupConfig> implements Serializable {
	
	private static Log log = LogFactory.getLog(ServiceConfig.class);
	
	private static final long serialVersionUID = -3167055235327688049L;

	private String serviceName;
	transient private TerminatorZkClient zkClient;
	transient private ServiceConfigSupport serviceConfigSupport;

	public ServiceConfig(String serviceName) {
		this.serviceName = serviceName;
	}

	public ServiceConfig(String serviceName, TerminatorZkClient zkClient,ServiceConfigSupport serviceConfigSupport) throws TerminatorZKException {
		this(serviceName);
		this.zkClient = zkClient;
		this.serviceConfigSupport = serviceConfigSupport;
		this.initConfig();
	}

	/**
	 * 备份序列化对象到本地文件系统
	 * 
	 * @param dir
	 * @throws IOException
	 */
	public static void backUp2LocalFS(ServiceConfig serviceConfig) throws IOException{
		ObjectOutputStream out = null;
		try{
			File file = new File("serviceConfig.bak");
			log.warn("备份ServiceConfig对象到本地文件系统 ==> " + file.getAbsolutePath());
			if(file.exists()){
				file.delete();
			}
			file.createNewFile();
			FileOutputStream fileOut = new FileOutputStream(file);
			out = new ObjectOutputStream(fileOut);
			out.writeObject(serviceConfig);
			out.flush();
		}finally{
			if(out != null)
				out.close();
		}
	}
	
	/**
	 * 从本地文件系统中读取备份的ServiceConfig对像
	 * 
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static ServiceConfig loadFromLocalFS() throws IOException, ClassNotFoundException{
		ObjectInputStream input = null;
		try{
			File file = new File("serviceConfig.bak");
			if(!file.exists()){
				throw new RuntimeException("找不到本地备份文件  ==> " + file.getAbsolutePath());
			}
			FileInputStream fileInput = new FileInputStream(file);
			input = new ObjectInputStream(fileInput);
			return (ServiceConfig)input.readObject();
		}finally{
			if(input!= null)
				input.close();
		}
	}
	
	private void initConfig() throws TerminatorZKException {
		log.warn("初始换ServicConfig对象.");
		String servicePath = TerminatorZKUtils.getMainPath(serviceName);
		boolean exists = zkClient.exists(servicePath);
		if(!exists){
			throw new TerminatorZKException("ZooKeeper上没有path ==> " + servicePath + "的znode结点，请检查serviceName ==> " + serviceName +" 是否正确.");
		}
		
		Watcher serviceWatcher = new ServiceWatcher(serviceName,zkClient,serviceConfigSupport); 
		
		List<String> groupList = zkClient.getChildren(servicePath,serviceWatcher);

		for (String groupName : groupList) {
			GroupConfig groupConfig = new GroupConfig(groupName);
			this.addGroupConfig(groupConfig);
			
			GroupWatcher groupWatcher = new GroupWatcher(zkClient,this,groupName);
			
			String groupPath = TerminatorZKUtils.contactZnodePaths(servicePath, groupName);
			List<String> hostList = zkClient.getChildren(groupPath,groupWatcher);
			for (String hostInfo : hostList) {
				HostConfig hostConfig = HostInfoParser.toHostConfig(hostInfo);
				groupConfig.addHostConfig(hostConfig);
			}
		}
		log.warn("ServiceConfig的结构如下:\n{\n" + this.toString() + "\n}\n");
	}

	/**
	 * 判断该搜索服务是否是单索引(无索引切分)搜索
	 * 
	 * @return
	 */
	public boolean isSingle() {
		return this.size() > 1 ? false : true;
	}

	/**
	 * 获取该搜索服务对应的所有的分组名称
	 * 
	 * @return
	 */
	public Set<String> getGroupNameSet() {
		return this.keySet();
	}

	/**
	 * 获取该搜索服务的所有机器节点的IP
	 * 
	 * @return
	 */
	public Set<String> getAllNodeIps() {
		Set<String> ipSet = new HashSet<String>();
		Set<String> groupNameSet = this.keySet();
		for (String groupName : groupNameSet) {
			GroupConfig groupConfig = this.get(groupName);
			ipSet.addAll(groupConfig.keySet());
		}
		return ipSet;
	}

	/**
	 * 获取单索引的coreName
	 * 
	 * @return
	 */
	public String getSingleCoreName() {
		return serviceName + TerminatorConstant.CORENAME_SEPERATOR + TerminatorConstant.SINGLE_CORE_GROUP_NAME;
	}

	/**
	 * 获取该搜索服务的搜有coreName
	 * 
	 * @return
	 */
	public Set<String> getCoreNameSet() {
		Set<String> groupNameSet = this.getGroupNameSet();
		Set<String> coreNameSet = new HashSet<String>(groupNameSet.size());
		for (String groupName : groupNameSet) {
			coreNameSet.add(serviceName + TerminatorConstant.HSF_VERSION_SEPERATOR + groupName);
		}
		return coreNameSet;
	}

	public void addGroupConfig(GroupConfig groupConfig) {
		this.put(groupConfig.getGroupName(), groupConfig);
	}

	public GroupConfig getGroupConfig(String groupName) {
		return this.get(groupName);
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}
	
	public int getGroupNum(){
		return this.size();
	}
	
	public void checkBySelf(){
		if(this.size() == 0){
			throw new RuntimeException("ServiceConfig配置对象理论上来讲有问题,没有任何的GroupConfig信息.");
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		Set<String> groupNameSet = this.keySet();
		sb.append(serviceName).append("\n");
		for (String groupName : groupNameSet) {
			GroupConfig groupConfig = this.getGroupConfig(groupName);
			sb.append(groupConfig).append("\n");
		}
		return sb.toString();
	}
}
