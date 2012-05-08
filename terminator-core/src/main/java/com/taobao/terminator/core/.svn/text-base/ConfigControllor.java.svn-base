package com.taobao.terminator.core;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.SolrResourceLoader;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.zk.TerminatorZKException;
import com.taobao.terminator.common.zk.TerminatorZKUtils;
import com.taobao.terminator.common.zk.TerminatorZkClient;
import com.taobao.terminator.core.wachers.TerminatorWatcher;

/**
 * 配置文件操作类 ==> 配置文件获取、写本地磁盘、备份、回滚备份 等操作
 * 
 * @author yusen
 */
public class ConfigControllor {
	protected static Log log = LogFactory.getLog(ConfigControllor.class);
	
	private TerminatorZkClient zkClient = null;
	private int configFileBackUpNum     = 3;
	
	private TerminatorWatcher  nodeWatcher = null;
	private TerminatorWatcher  coreWatcher = null;
	private TerminatorWatcher  configWatcher = null;
	
	private static  DateFormat tmpFormat = new SimpleDateFormat("yyyyMMddhhmmss");
	
	protected static String solrHome = SolrResourceLoader.locateSolrHome();
	
	public ConfigControllor(TerminatorZkClient zkClient,TerminatorWatcher nodeWatcher,TerminatorWatcher coreWatcher,TerminatorWatcher configWatcher){
		this.zkClient = zkClient;
		this.nodeWatcher = nodeWatcher;
		this.coreWatcher = coreWatcher;
		this.configWatcher = configWatcher;
		
		this.nodeWatcher.setConfigControllor(this);
		this.coreWatcher.setConfigControllor(this);
		this.configWatcher.setConfigControllor(this);
	}
	
	
	/**
	 * 获取所有SolrCore的配置文件 ,不删除多余的目录
	 * 
	 */
	public void fetchConf() throws TerminatorZKException, IOException{
		this.fetchConf(false);
	}
	
	/**
	 * 获取所有SolrCore的配置文件
	 * 
	 * @param delNeedless 是否删除多余的配置目录
	 */
	public void fetchConf(boolean delNeedless) throws TerminatorZKException, IOException{
		log.warn("从ZooKeeper上获取最新的配置文件.");
		String nodepath  = TerminatorZKUtils.getNodePath(TerminatorCommonUtils.getLocalHostIP());
		
		if (!zkClient.exists(nodepath)) {
			zkClient.create(nodepath, new byte[0]);
		}
		
		final List<String> coreList = zkClient.getChildren(nodepath,nodeWatcher);
		
		File coresDir = new File(solrHome);
		for(String coreName : coreList){
			this.fetchCore(coreName);
		}
		
		if(delNeedless){
			log.warn("删除多余的、不必要的SolrCore的配置信息.");
			File[] needDelCoreDirs = coresDir.listFiles(new FileFilter(){
				@Override
				public boolean accept(File file) {
					return isCorrectCoreDirInSolrHome(file) && !coreList.contains(file.getName());
				}
			});
			
			if(needDelCoreDirs != null && needDelCoreDirs.length > 0){
				for(File file : needDelCoreDirs){
					log.warn("删除目录文件 ==> " + file.getAbsolutePath());
					FileUtils.deleteDirectory(file);
				}
			}
		}
	}
	
	/**
	 * 判断一个File是否是正确的SolrCore的配置文件的目录文件
	 * 
	 * @param file
	 * @return
	 */
	public static boolean isCorrectCoreDirInSolrHome(File file){
		return file.isDirectory() && TerminatorCommonUtils.isCorrectCoreName(file.getName());
	}
	
	/**
	 * 删除一个Core对应的目录文件
	 * 
	 * @param coreName
	 * @throws IOException
	 */
	public void deleteCore(String coreName) throws IOException{
		File coreDir = new File(solrHome,coreName);
		if(coreDir.exists() && coreDir.isDirectory()){
			FileUtils.deleteDirectory(coreDir);
		}
	}
	
	/**
	 * 删除Zk上一个Core对应的znode节点
	 * 
	 * @param coreName
	 * @throws TerminatorZKException
	 */
	public void deleteCoreFromZk(String coreName) throws TerminatorZKException{
		String path = generateCorePath(coreName);
		
		log.warn("从ZK上删除名为[" + coreName +"] 的配置信息.");
		if(zkClient.exists(path)){
			zkClient.rdelete(path);
		}
	}
	
	/**
	 * 获取一个Core的配置文件
	 * 
	 * @param coreName 
	 * @throws TerminatorZKException
	 * @throws IOException
	 */
	public void fetchCore(String coreName) throws TerminatorZKException,IOException {
		String path = generateCorePath(coreName);
		log.warn("\t获取名为 [" + coreName +"]的SolrCore的配置文件. znode的path ==> " + path);
		
		List<String> configList = zkClient.getChildren(path, coreWatcher);
		
		/*
		 * 发布端由于不是一个事务，而是一层一层的创建path，然后在一层一层的设置Data，
		 * 故从事时序上来讲，很有可能父节点已经存在了，但是孩子节点还在创建中
		 * 为了防止这个问题，采用这种方式
		 * */
		while(configList.size() <= 0){
			configList = zkClient.getChildren(path,coreWatcher);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				log.error(e,e);
			}
		}
		
		for (String configName : configList) {
			this.fetchOneConfigFile(coreName, configName);
		}
	}
	
	/**
	 * 生成core的path
	 * 
	 * @param coreName
	 * @return
	 */
	public static String generateCorePath(String coreName){
		String nodepath = TerminatorZKUtils.getNodePath(TerminatorCommonUtils.getLocalHostIP());
		return TerminatorZKUtils.contactZnodePaths(nodepath, coreName);
	}
	
	/**
	 * 生成某个core的具体某个配置文件的路径
	 * 
	 * @param coreName
	 * @param configName
	 * @return
	 */
	public static String generateConfigPath(String coreName,String configName){
		String nodepath = TerminatorZKUtils.getNodePath(TerminatorCommonUtils.getLocalHostIP());
		String corepath = TerminatorZKUtils.contactZnodePaths(nodepath, coreName);
		return TerminatorZKUtils.contactZnodePaths(corepath, configName);
	}
	
	
	/**
	 * 获取某SolrCore下的名为configName的配置文件
	 * 
	 * @param coreName
	 * @param configName
	 * @throws TerminatorZKException
	 * @throws IOException
	 */
	public void fetchOneConfigFile(String coreName,String configName) throws TerminatorZKException, IOException{
		String configpath = generateConfigPath(coreName,configName);
		log.warn("获取名为 [" + coreName +"] 的配置文件 ==> " + configName +", znode的path为 ==> " + configpath);
		
		byte[] data = zkClient.getData(configpath, configWatcher);
		this.writeConfFile2SolrDir(coreName, configName, data);
	}
	
	/**
	 * 删除某SolrCore下的名为configName的配置文件
	 * 
	 * @param coreName
	 * @param configName
	 */
	public void deleteConfigFile(String coreName,String configName){
		String configFilePath = coreName + File.separator + "conf" + File.separator + configName;
		File file = new File(solrHome,configFilePath);
		if(file.exists() && file.isFile()){
			file.delete();
		}
	}
	
	/**
	 * 从ZK上删除某个Core的某个配置文件
	 * 
	 * @param coreName
	 * @param configName
	 * @throws TerminatorZKException
	 */
	public void deleteConfigFileFormZk(String coreName,String configName) throws TerminatorZKException{
		String path = generateConfigPath(coreName,configName);
		log.warn("从ZK上删除CoreName ==> " + coreName + " 配置文件名 ==> " + configName + "的配置文件");
		if(zkClient.exists(path)){
			zkClient.delete(path);
		}
	}
	
	/**
	 * 更新某SolrCore下的名为configName的配置文件的内容为newData
	 * 
	 * @param coreName
	 * @param configName
	 * @param newData
	 * @throws IOException
	 */
	public void updateConfigFile(String coreName,String configName,byte[] newData) throws IOException{
		String configFilePath = coreName + File.separator + "conf" + File.separator + configName;
		File file = new File(solrHome,configFilePath);
		if(file.exists()){
			FileUtils.writeByteArrayToFile(file, newData);
		}
	}
	
	/**
	 * 写配置文件 ==> 有备份机制，备份老的文件（文件名.时间戳的形式，备份最近修改的3份），写入新的文件
	 * 
	 * @param coreName
	 * @param fileName
	 * @param data
	 * @throws IOException
	 */
	public void writeConfFile2SolrDir(String coreName,String fileName ,byte[] data) throws IOException{
		File coreDir = this.getCoreDir(coreName);
		
		File configFile = new File(coreDir,fileName);
		
		boolean exists = configFile.exists();
		if(exists){
			//备份文件 fileName.timeStamp
			this.backUpFile(coreDir, fileName);
			configFile.delete();
		}
		
		try{
			log.warn("写新的配置文件 ==> [ " + configFile.getPath() +" ]" );
			FileUtils.writeByteArrayToFile(configFile, data);
		}catch(IOException e){
			if(!exists){
				throw e;
			}
			log.error("写新的文件产生了异常，故回滚到最近一次备份的文件.");
			File f = this.rollbackFile(coreDir, fileName);
			log.warn("回滚的文件为[" + f.getPath() + "]");
		}
	}
	
	/**
	 * 读取某SolrCore下某名为configName的配置文件的内容 以InputStream的形式返回
	 * 
	 * @param coreName
	 * @param configName
	 * @return
	 * @throws FileNotFoundException
	 */
	public InputStream readConfigFileFromSolrDir(String coreName,String configName) throws FileNotFoundException{
		File coreDir = this.getCoreDir(coreName);
		File file = new File(coreDir,configName);
		InputStream input = new FileInputStream(file);
		return input;
	}
	
	/**
	 * 获取某SolrCore的目录
	 * 
	 * @param coreName
	 * @return
	 */
	private File getCoreDir(String coreName){
		File solrHomeDir = new File(solrHome);
		if(!solrHomeDir.exists()){
			solrHomeDir.mkdirs();
		}
		
		String configPath = coreName + File.separator + "conf";
		File coreDir = new File(solrHomeDir,configPath);
		if(!coreDir.exists()){
			coreDir.mkdirs();
		}
		return coreDir;
	}
	
	/**
	 * 回滚有修改的配置文件
	 * 
	 * @param dir
	 * @param fileName
	 * @return
	 */
	public File rollbackFile(File dir, final String fileName) {
		File srcFile = new File(dir,fileName);
		
		log.warn("回滚文件 ==> " + srcFile.getPath());
		
		File[] fileList =this.getBakFiles(dir, fileName);
		if(fileList == null || fileList.length <= 0){
			log.error("文件名为[" + fileName +"] 的备份文件不存在,不能回滚.");
			return null;
		}
		
		List<File> sortedFileList = this.sortBakFile(fileList);
		
		File file = sortedFileList.get(0);
		
	
		if(srcFile.exists()){
			log.warn("删除现有的文件[" + srcFile.getPath() +"].");
			srcFile.delete();
		}
		
		file.renameTo(new File(dir,fileName));
		return file;
	}
	
	public File rollbackFile(String coreName,String fileName){
		File coreDir = this.getCoreDir(coreName);
		return this.rollbackFile(coreDir, fileName);
	}
	
	public void rollbackToZk(String coreName,String fileName) throws IOException, TerminatorZKException{
		File dir = this.getCoreDir(coreName);
		File file = new File(dir,fileName);

		if(file.exists()){
			byte[] data = FileUtils.readFileToByteArray(file);
			String path = generateConfigPath(coreName, fileName);
			
			log.warn("将本地文件系统中的文件 ==> " + file.getAbsolutePath() +" 写入ZK，对应的znode的path ==>" + path);
			
			if(zkClient.exists(path)){
				zkClient.setData(path, data);
				log.warn("写入成功.");
			}
		}
	}
	
	
	private File[] getBakFiles(File dir,final String fileName){
		return  dir.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return file.getName().startsWith(fileName) && !file.getName().equals(fileName);
			}
		});
	}
	
	/**
	 * 按备份文件的时间戳后缀有大到小排序
	 * 
	 * @param bakFileList
	 * @return
	 */
	private List<File> sortBakFile(File[] bakFileList){

		List<File> sortedFileList = Arrays.asList(bakFileList);
		
		Collections.sort(sortedFileList, new Comparator<File>() {
			public int compare(File file1, File file2) {
				String fileName1 = file1.getName();
				String fileName2 = file2.getName();
				
				String timeStamp1 = fileName1.substring(fileName1.lastIndexOf(".") + 1);
				String timeStamp2 = fileName1.substring(fileName2.lastIndexOf(".") + 1);
				
				long time1 = 0L;
				
				try{
					time1 = Long.valueOf(timeStamp1);
				}catch(NumberFormatException e){
					return -1;
				}
				
				long time2 = 0L;
				try{
					time2 = Long.valueOf(timeStamp2);
				}catch(NumberFormatException e){
					return -1;
				}
				
				return time1 > time2 ? -1 : 1;
			}
		});
		
		return sortedFileList;
	}
	
	/**
	 * 备份dir目录下的名为fileName文件
	 * 
	 * @param dir
	 * @param fileName
	 * @throws IOException
	 */
	public void backUpFile(File dir,final String fileName) throws IOException{
		//back up the config file like this ==> fileName.timeStamp
		File configFile = new File(dir,fileName);
		String bakFileName = genBakFileName(configFile.getName());
		
		File bakFile =  new File(dir,bakFileName);
		log.warn("备份文件,源文件 [" + configFile.getPath() +"] 目标备份文件 [" + bakFile.getPath()+"]");
		FileUtils.copyFile(configFile, bakFile);
		
		File[] bakFileList = this.getBakFiles(dir, fileName);
		
		//clear up 
		if(bakFileList.length > configFileBackUpNum){
			log.warn("清理多余的备份文件，保持备份文件数为 [" + configFileBackUpNum +"]");
			List<File> sortedFileList = this.sortBakFile(bakFileList);
			
			for(int i = configFileBackUpNum ;i < sortedFileList.size() ;i++){
				File f = sortedFileList.get(i);
				if(f.exists()){
					f.delete();
					log.warn("删除配置文件备份  ==>  [" + f.getPath() +"]");
				}
			}
		}
	}
	
	public static String genBakFileName(String fileName){
		return fileName + "." + tmpFormat.format(new Date());
	}
	
	public static void main(String[] args) throws IOException {
		System.setProperty("solr.solr.home","D:\\all-nodes\\solr_home");
		ConfigControllor cc = new ConfigControllor(null, null, null, null);
//		cc.writeConfFile2SolrDir("search4album-0", "solrconfig.xml", new String("s hello hello hello").getBytes());
		cc.rollbackFile("search4album-0", "solrconfig.xml");
		
	}


	public TerminatorZkClient getZkClient() {
		return zkClient;
	}

	public void setZkClient(TerminatorZkClient zkClient) {
		this.zkClient = zkClient;
	}

	public int getConfigFileBackUpNum() {
		return configFileBackUpNum;
	}

	public void setConfigFileBackUpNum(int configFileBackUpNum) {
		this.configFileBackUpNum = configFileBackUpNum;
	}

	public TerminatorWatcher getNodeWatcher() {
		return nodeWatcher;
	}

	public void setNodeWatcher(TerminatorWatcher nodeWatcher) {
		this.nodeWatcher = nodeWatcher;
	}

	public TerminatorWatcher getCoreWatcher() {
		return coreWatcher;
	}

	public void setCoreWatcher(TerminatorWatcher coreWatcher) {
		this.coreWatcher = coreWatcher;
	}

	public TerminatorWatcher getConfigWatcher() {
		return configWatcher;
	}

	public void setConfigWatcher(TerminatorWatcher configWatcher) {
		this.configWatcher = configWatcher;
	}

	public static DateFormat getTmpFormat() {
		return tmpFormat;
	}

	public static void setTmpFormat(DateFormat tmpFormat) {
		ConfigControllor.tmpFormat = tmpFormat;
	}
	
}
