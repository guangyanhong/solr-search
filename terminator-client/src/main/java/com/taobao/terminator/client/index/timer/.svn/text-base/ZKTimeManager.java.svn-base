package com.taobao.terminator.client.index.timer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.zk.TerminatorZKException;
import com.taobao.terminator.common.zk.TerminatorZKUtils;
import com.taobao.terminator.common.zk.TerminatorZkClient;

/**
 * 通过ZK统一管理增量时间     单例模式
 * 
 * @author yusen
 */
public class ZKTimeManager implements TimerManager {
	protected static Log logger = LogFactory.getLog(TimerManager.class);
	private static String timeRootPath = TerminatorZKUtils.contactZnodePaths(TerminatorZKUtils.TERMINATOR_ROOT_PATH, TerminatorZKUtils.TIME_ROOT_PATH);
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	private TerminatorZkClient zkClient;
	private String serviceName = null;
	
	/* 增量的开始和结束时间点 */
	private Date startTime; 
	private Date endTime;
	
	private boolean isInited = false;
	
	private static Map<String,ZKTimeManager> zkTimeManagers = null;
	
	private ZKTimeManager(TerminatorZkClient zkClient,String serviceName) throws TimeManageException {
		if(zkClient == null){
			throw new TimeManageException("ZKTimeManager的构造参数TerminatorZkClient不能为null.");
		}
		this.zkClient = zkClient;
		this.serviceName = serviceName;
		this.initZnode();
	}
	
	private void initZnode() throws TimeManageException{
		logger.warn("初始化时间节点  ==> " + timeRootPath);
		try {
			if(!this.zkClient.exists(timeRootPath)){
				this.zkClient.createPath(timeRootPath);
			}
		} catch (TerminatorZKException e) {
			throw new TimeManageException("初始化时间节点失败 path ==> " + timeRootPath,e);
		}
	}
	
	/**
	 * 创建ZKTimeManager对象
	 * 
	 * @param zkClient
	 * @param serviceName
	 * @throws TimeManageException
	 */
	public static void createInstance(TerminatorZkClient zkClient,String serviceName) throws TimeManageException {
		logger.warn("创建ZKTimeManager对象.");
		if(zkTimeManagers == null){
			zkTimeManagers = new HashMap<String,ZKTimeManager>();
		}
		
		if(zkTimeManagers.containsKey(serviceName)){
			return;
		}else{
			zkTimeManagers.put(serviceName, new ZKTimeManager(zkClient,serviceName));
		}
	}
	
	/**
	 * 获取ZKTimeManager对象实例<bt>
	 * 
	 * PS.调用该方法前请确认已经在某个地方调用过createInstance(TerminatorZkClient zkClient,String serviceName)方法
	 * 
	 * @return
	 * @throws TimeManageException
	 */
	public static ZKTimeManager getInstance(String serviceName)throws TimeManageException {
		if(zkTimeManagers == null){
			throw new TimeManageException("ZKTimeManager对象还没有实例化，请确保调用getInstance()方法前已经调用了 createInstance(TerminatorZkClient zkClient)方法.");
		}
		
		ZKTimeManager m = zkTimeManagers.get(serviceName);
		if(m == null){
			throw new TimeManageException("ZKTimeManager对象还没有实例化，请确保调用getInstance()方法前已经调用了 createInstance(TerminatorZkClient zkClient)方法.");
		}
		return m;
	}
	
	/**
	 * 单纯的获取增量的开始和结束时间点
	 * 
	 */
	public StartAndEndTime justGetTimes()throws TimeManageException {
		if(!isInited) 
			throw new TimeManageException("时间还未初始化，请确保已经调用了initTimes()方法.");
		return new StartAndEndTime(this.startTime,this.endTime);
	}
	
	/**
	 * 获取增量的开始和结束时间点
	 */
	public StartAndEndTime initTimes() throws TimeManageException {
		this.endTime = new Date();
		String path = this.genPath(serviceName);
		String dateStr = null;
		try {
			if(zkClient.exists(path)){
				byte[] data = zkClient.getData(path);
				dateStr = TerminatorZKUtils.toString(data);
				this.startTime = TerminatorCommonUtils.parseDate(dateStr);
			}else{
				//第一次增量，则以当天的0点作为起始时间
				String d = df.format(this.endTime);
				this.startTime = TerminatorCommonUtils.parseDate(d + " 00:00:00");
			}
		} catch (TerminatorZKException e) {
			throw new TimeManageException("获取service时间时异常  coreName ==> " + serviceName,e);
		} catch (ParseException e) {
			throw new TimeManageException("获取service时间时异常  [时间格式转换异常] coreName ==> " + serviceName + "  date-string ==> " + dateStr,e);
		}
		isInited = true;
		return new StartAndEndTime(this.startTime,this.endTime);
	}

	/**
	 * 重置ZooKeeper上的增量时间点，设置为本次的endTime的值
	 */
	public StartAndEndTime resetTimes()throws TimeManageException  {
		if(!isInited) 
			throw new TimeManageException("时间还未初始化，请确保已经调用了initTimes()方法.");
		String path = this.genPath(serviceName);
		String dateStr = TerminatorCommonUtils.formatDate(this.endTime);
		byte[] dateByte = TerminatorZKUtils.toBytes(dateStr);
		
		logger.warn("设置service的时间,path ==> " + path + " date-string ==> " + dateStr);
		try {
			if(!zkClient.exists(path)){
				zkClient.create(path, dateByte);
			}else{
				zkClient.setData(path, dateByte);
			}
		} catch (TerminatorZKException e) {
			throw new TimeManageException("设置servic时间时异常  coreName ==> " + serviceName + "date-string ==> " + dateStr,e);
		}
		return this.justGetTimes();
	}
	
	/**
	 * 强制设置ZK上的时间 -- 一般不推荐使用
	 * 
	 * @param date
	 * @return
	 * @throws TimeManageException
	 */
	@Deprecated
	public Date forceSetTime(Date date) throws TimeManageException  {
		String path = this.genPath(serviceName);
		String dateStr = TerminatorCommonUtils.formatDate(date);
		byte[] dateByte = TerminatorZKUtils.toBytes(dateStr);
		
		logger.warn("设置service的时间,path ==> " + path + " date-string ==> " + dateStr);
		try {
			if(!zkClient.exists(path)){
				zkClient.create(path, dateByte);
			}else{
				zkClient.setData(path, dateByte);
			}
		} catch (TerminatorZKException e) {
			throw new TimeManageException("设置servic时间时异常  coreName ==> " + serviceName + "date-string ==> " + dateStr,e);
		}
		return date;
	}
	
	private String genPath(String coreName){
		return TerminatorZKUtils.contactZnodePaths(timeRootPath, coreName);
	}
}
