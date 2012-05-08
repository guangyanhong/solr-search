package com.taobao.terminator.client.index;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.client.index.buffer.DataBuffer;
import com.taobao.terminator.client.index.buffer.DataBuffer.CapacityInfo;
import com.taobao.terminator.client.index.data.DataProvider;
import com.taobao.terminator.client.index.data.DataProviderException;
import com.taobao.terminator.client.index.data.procesor.DataProcessor;
import com.taobao.terminator.client.index.data.procesor.DeletionDataProcessor;
import com.taobao.terminator.client.index.data.procesor.DataProcessor.ResultCode;
import com.taobao.terminator.client.index.data.xml.SolrXmlDocGenerator;
import com.taobao.terminator.client.index.timer.TimeManageException;
import com.taobao.terminator.client.index.timer.ZKTimeManager;
import com.taobao.terminator.client.index.timer.TimerManager.StartAndEndTime;
import com.taobao.terminator.client.index.transmit.HSFIndexTransmitor;
import com.taobao.terminator.client.index.transmit.IndexTransmitException;
import com.taobao.terminator.client.index.transmit.IndexTransmitor;
import com.taobao.terminator.client.router.GroupRouter;
import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.config.ServiceConfig;
import com.taobao.terminator.common.constant.IndexType;

/**
 * 客户端索引源数据的生产主控对象，调用DataProvider获取数据，并写入DataBuffer，到达DataBuffer上限时通过IndexTransmitor对象进行批量的传输
 * 
 * @author yusen
 */
public class TerminatorIndexProvider implements IndexProvider{
	protected static Log log = LogFactory.getLog(IndexProvider.class);

	private SolrXmlDocGenerator         xmlGenerator = new SolrXmlDocGenerator();; 
	private DataProvider                dataProvider;  
	private boolean                     isDataProviderInitSuc = false;
	private DataProcessor               dataProcessor;    
	private Map<String,DataBuffer>      dataBuffers;      
	private Map<String,IndexTransmitor> indexTramsmitors;
	private Set<String>                 illegalTransmitorNames = null;
	private FetchDataExceptionHandler   fetchDataExceptionHandler = null;

	private CapacityInfo  capacityInfo;
	private ServiceConfig serviceConfig;
	private String        serviceName;
	private GroupRouter   router;
	private IndexType     indexType;
	private Set<String>   groupNameSet; 
	
	private AtomicBoolean isDumping = new AtomicBoolean(false);
	
	private Timer timer = new Timer(Timer.SECOND);
	private boolean inited  = false;
	
	/**
	 * 所有的必要的属性设置完毕之后调用此方法进行初始化
	 */
	public void afterPropertiesSet(){
		this.checkProperties();
		
		this.groupNameSet = serviceConfig.getGroupNameSet();
		int groupNum = serviceConfig.getGroupNum();
		if(dataBuffers == null){
			dataBuffers = new HashMap<String,DataBuffer>(groupNum);
		}
		
		if(indexTramsmitors == null){
			indexTramsmitors = new HashMap<String,IndexTransmitor>(groupNum);
		}
		
		if(illegalTransmitorNames == null){
			illegalTransmitorNames = new HashSet<String>();
		}
		
		if(fetchDataExceptionHandler == null){
			fetchDataExceptionHandler = new FetchDataExceptionHandler() {
				@Override
				public void handle(Exception e) {
					log.fatal(" ====> DataProvider获取数据异常  <==== ",e);
				}
			};
		}
		
		for(String groupName : this.groupNameSet){
			DataBuffer dataBuffer = dataBuffers.get(groupName);
			if(dataBuffer == null){
				dataBuffer = new DataBuffer(capacityInfo);
				dataBuffers.put(groupName, dataBuffer);
			}
			
			IndexTransmitor indexTransmitor = indexTramsmitors.get(groupName);
			if(indexTransmitor == null){
				indexTransmitor =this.createIndexTransmitor(serviceName, groupName, indexType);
				indexTramsmitors.put(groupName, indexTransmitor);
			}
		}
		inited = true;
	}
	
	protected IndexTransmitor createIndexTransmitor(String serviceName,String groupName,IndexType indexType){
		return new  HSFIndexTransmitor(serviceName, groupName,indexType);
	}
	
	private void checkProperties(){
		if(serviceConfig == null || capacityInfo == null || indexType == null || dataProvider == null){
			throw new IllegalArgumentException("属性 serviceConfig capacityInfo indexType dataProvider 必须注入才能调用此方法.");
		}
	}
	
	private void resetGroupNameSet(){
		this.groupNameSet = serviceConfig.getGroupNameSet();
	}

	/**
	 * 开始此次Dump，进行一些准备工作
	 */
	protected void startDump(){
		if(!inited) 
			return;

		isDataProviderInitSuc = false;
		
		if(isDumping.getAndSet(true)){
			RuntimeException e =  new RuntimeException("有 " + indexType + " 任务正在进行，故不能处理此次dump请求.");
			log.error(e,e);
			throw e;
		}
		
		log.warn("开始Dump数据，dump类型 ==> " + indexType + " 并进行一系列的Dum前的准备工作.");
		timer.start();
		if(indexType == IndexType.INCREMENT){
			log.warn("此次Dump为增量Dump,初始化StartTime和EndTime(从ZK上获取时间数据)");
			try {
				StartAndEndTime times = ZKTimeManager.getInstance(this.serviceName).initTimes();
				log.warn("此次增量的起始结束时间点分别为   ==> " + TerminatorCommonUtils.formatDate(times.startTime) + "     " + TerminatorCommonUtils.formatDate(times.endTime));
			} catch (Exception e2) {
				log.error("从ZK上获取增量时间点失败,此次增量的工作一开始就宣告失败.",e2);
				isDumping.set(false);
				return ;
			}
		}
		
		try {
			dataProvider.init();
			isDataProviderInitSuc = true;
		} catch (Exception e) {
			log.error("DataProvider初始化失败",e);
			isDataProviderInitSuc = false;
			isDumping.set(false);
			return;
		}
		
		resetGroupNameSet();
		for(String groupName : this.groupNameSet){
			log.warn("Dump准备工作, groupName ==> " + groupName);
			DataBuffer dataBuffer = dataBuffers.get(groupName);
		    dataBuffer.reset();
			
		    IndexTransmitor indexTransmitor = indexTramsmitors.get(groupName);
		    if(indexTransmitor == null){
				indexTransmitor =this.createIndexTransmitor(serviceName, groupName, indexType);
				indexTramsmitors.put(groupName, indexTransmitor);
			}
		    
		    boolean startSuc = false;
			try {
				startSuc = indexTransmitor.start();
			} catch (Exception e) {
				log.error("调用Server端的start方法失败,故接下来的针对该节点的数据传送都不进行",e);
				illegalTransmitorNames.add(groupName);
				continue;
			}
			if(!startSuc){
				illegalTransmitorNames.add(groupName);
			}
		}
		
		if(!illegalTransmitorNames.isEmpty()){
			log.error("Dump前的准备工作(调用Service的start方法)失败的有:" + illegalTransmitorNames + "  故后续的传输和扫尾工作将不进行.");
		}
	}
	
	/**
	 * dump的主要流程
	 */
	public void dump() {
		this.startDump();
		
		int count = 0;
		int failedCount = 0;
		boolean isOk = true;
		try{
			if(!isDataProviderInitSuc){
				log.fatal("初始化DataProvider失败，故直接结束此次Dump任务.");
				isOk = false;
				return;
			}
			
			if(groupNameSet.size() == illegalTransmitorNames.size()){
				log.fatal("所有的Group的Start方法调用均失败了，直接结束此次dump任务，没有必要进行接下来的流程.");
				isOk = false;
				return;
			}
			
			long startTime = System.currentTimeMillis();
			long s = startTime; 
 			while (dataProvider.hasNext()) {
				Map<String,String> row = null;
				try{
					row = dataProvider.next();
				}catch(DataProviderException e){
					failedCount ++;
					continue;
				}
				
				count ++;
				String groupName = router.getGroupName(row);
				if(illegalTransmitorNames.contains(groupName)){//调用start方法就失败了，故也没有必要继续传输数据了
					continue;
				}
				
				if(count % 10000 == 0){
					log.warn("Dump 1万条记录花费的时间 :" + (System.currentTimeMillis() - startTime)/1000 + "s  目前dump耗费总时间为 : " + (System.currentTimeMillis() - s)/1000 +  "s 目前dump的总记录数为 : " + count);
					startTime = System.currentTimeMillis();
				}
				
				if(dataProcessor != null){
					try {
						ResultCode rs = dataProcessor.process(row);
						if(rs == null || !rs.isSuc()){
							log.debug("摒弃数据,原因  ==>" + (rs != null ? rs.toString():" ResultCode is nulll ") + "   \n数据描述 ==> " + row.toString());
							failedCount ++;
							continue;
						}
					} catch (Exception e) {
						log.error("DataProcessor处理数据异常,忽略此条数据,data ==> " + row ,e);
						failedCount ++;
						continue;
					}
				}
				
				byte[] data = null;
				try{//区分动作 是删除还是更新(Add和Update操作是一回事儿)
					if(row.containsKey(DeletionDataProcessor.DELETION_KEY)){
						if(indexType == IndexType.INCREMENT){
							data = xmlGenerator.genSolrDeleteXMLByUniqueKey(row.get(DeletionDataProcessor.DELETION_KEY)).getBytes("GBK");
						}else{ //全量的时候，删除的数据直接扔掉
							continue;
						}
					}else{
						data = xmlGenerator.genSolrUpdateXML(row, IndexType.INCREMENT.equals(indexType)).getBytes("GBK");
					}
				} catch (Exception e) {
					log.error("XmlGenerator生产xml失败,忽略此条数据 ,data ==> " + row,e);
					failedCount ++;
					continue;
				}
				
				this.appendBuffer(groupName,data);
			}
		}catch(Exception e){
			isOk = false;
			log.error("TerminatorIndexProvider获取数据异常",e);
			this.handleFetchDataException(e);
		} finally{
			try{
				this.finishDump(isOk);
				this.dataProvider.close();
				timer.end();
			}catch(Exception e){
				log.error("Dump的结束扫尾过程出现异常",e);
			} finally{
				illegalTransmitorNames.clear();
				//无论如何这个状态要设置，以免造成一次失败，后边全部拒绝掉的惨状
				isDumping.set(false);
			}
			
			log.warn( (isOk ? "【正常】":"【异常】" )+ "结束此次Dump数据过程，总耗时 ：" + timer.getConsumeTime() + " s  平均耗时: " + timer.getAverageTime() +" s  累计执行次数：" + timer.getTotalTimes() + " 此次Dump的数据总量 : " + count +"  失败的总量：" + failedCount);
		}
	}
	
	protected void handleFetchDataException(Exception e){
		if(fetchDataExceptionHandler != null){
			fetchDataExceptionHandler.handle(e);
		}
	}
	
	/**
	 * 结束此次Dump，做一些扫尾清理工作
	 */
	protected void finishDump(boolean isOk){
		log.warn("Dump数据基本完成，进行一些列的后续操作,如finish通知,清空本地缓存,重置DataProvider等操作.");
		
		for(String groupName : this.groupNameSet){
			if(illegalTransmitorNames.contains(groupName)){
				log.fatal("由于此调用在start方法调用的时候就失败了，故后续的transmit和finish操作将不能进行，忽略！");
				continue;
			}
			log.warn("Dump结束的扫尾清理工作, groupName ==> " + groupName);
			IndexTransmitor indexTransmitor = indexTramsmitors.get(groupName);
			if(indexTransmitor == null){
				indexTransmitor =this.createIndexTransmitor(serviceName, groupName, indexType);
				indexTramsmitors.put(groupName, indexTransmitor);
			}
			try {
				DataBuffer dataBuffer = dataBuffers.get(groupName);
				int len = 0;
				if((len=dataBuffer.getBufferUsage()) > 0){ //Buffer中依然有数据，才传输
					byte[] data = new byte[len];
					System.arraycopy(dataBuffer.getBuffer(), 0, data, 0, len);
					indexTransmitor.transmit(data);
				}
				dataBuffer.reset();
				indexTransmitor.finish();
			} catch (Exception e) {
				log.error("IndexTransmitor.finish()失败，groupName为   " + groupName,e);
			}
		}
		
		illegalTransmitorNames.clear();
		try {
			dataProvider.close();
		} catch (DataProviderException e1) {
			log.error("此次Dump结束，关闭DataProvider失败 ",e1);
		}
		
		if(indexType == IndexType.INCREMENT && isOk){
			try {
				log.warn("此次Dump为增量Dump,重新设置StartTime和EndTime(写ZK上的相应的znode节点)");
				StartAndEndTime times = ZKTimeManager.getInstance(this.serviceName).resetTimes();
				log.warn("结束此增量并重置ZK上的增量时间点   ==> " + TerminatorCommonUtils.formatDate(times.startTime) + "     " + TerminatorCommonUtils.formatDate(times.endTime));
			} catch (TimeManageException e) {
				log.error("重新设置ZK上的增量时间点失败,可能下次增量会和本次增量有重叠，但是不会造成索引数据的重复.");
			}
		}
		
		isDumping.set(false);
	}
	
	/**
	 * 将数据追加到本地内存的Buffer中
	 * 
	 * @param groupName
	 * @param data
	 */
	private void appendBuffer(String groupName,byte[] data){
		DataBuffer buffer = dataBuffers.get(groupName);
		if(buffer == null){
			buffer = new DataBuffer(capacityInfo);
			dataBuffers.put(groupName, buffer);
		}
		
		buffer.append(data);
		if(buffer.isOverFlow()){
			log.warn("本地Buffer已满   最大容量[" + buffer.getMaxCapacity() +"] 现在已使用 [" + buffer.getBufferUsage() +"],发送这批数据到IndexWrite的机器 ,groupName ==> " + groupName);
			int l = buffer.getBufferUsage();
			byte[] bytes = new byte[l];
			System.arraycopy(buffer.getBuffer(), 0, bytes, 0, l);
			this.transmit(groupName,bytes);
			buffer.reset();
		}
	}
	
	/**
	 * 传递数据到Terminator写索引的服务器
	 * 
	 * @param groupName
	 * @param data
	 */
	private void transmit(String groupName,byte[] data){
		if(illegalTransmitorNames.contains(groupName)){
			log.fatal("由于此调用在start方法调用的时候就失败了，故后续的transmit和finish操作将不能进行，忽略！");
			return;
		} 

		IndexTransmitor indexTransmitor = indexTramsmitors.get(groupName);
		if(indexTransmitor == null){
			indexTransmitor = this.createIndexTransmitor(serviceName, groupName, indexType);
			indexTramsmitors.put(groupName, indexTransmitor);
		}
		
		try {
			indexTransmitor.transmit(data);
		} catch (IndexTransmitException e) {
			log.error("发送数据失败，groupName ==> " + groupName,e);
		}
	}
	
	/**
	 *  执行过程 计时器
	 * @author yusen
	 */
	public class Timer{
		public static final int MILLION_SECOND = 1;
		public static final int SECOND = 2;
		public static final int MINUTE = 3;
		
		private int  timeUnit = SECOND;
		private long startTime;
		private long endTime;
		private long cosumeTime ;
		private int  totalTimes;
		private long averageTime;
		private boolean isStarted = false;
		private boolean isEnded   = true;
		
		public Timer(){}
		
		public Timer(int timeUnit){
			this.timeUnit = timeUnit;
		}
		
		public void start(){
			if(!isEnded) return ;
			isStarted = true;
			startTime = System.currentTimeMillis();
		}
		
		public void end(){
			if(!isStarted) return;
			endTime = System.currentTimeMillis();
			cosumeTime = endTime-startTime;
			totalTimes ++;
			averageTime = (averageTime * (totalTimes - 1) + cosumeTime) / totalTimes;
			isEnded = true;
		}
		
		public long getConsumeTime(int timeUnit) {
			switch (timeUnit) {
			case MILLION_SECOND:
				return cosumeTime;
			case SECOND:
				return cosumeTime / 1000;
			case MINUTE:
				return cosumeTime / 1000 / 60;
			default:
				return cosumeTime;
			}
		}
		
		public long getConsumeTime(){
			return this.getConsumeTime(timeUnit);
		}
		
		public long getAverageTime(int timeUnit){
			switch (timeUnit) {
			case MILLION_SECOND:
				return averageTime;
			case SECOND:
				return averageTime / 1000;
			case MINUTE:
				return averageTime / 1000 / 60;
			default:
				return averageTime;
			}
		}
		
		public long getAverageTime(){
			return this.getAverageTime(timeUnit);
		}
		
		public void clear(){
			totalTimes = 0;
			averageTime = 0;
		}
		
		public int getTotalTimes(){
			return totalTimes;
		}
	}

	public SolrXmlDocGenerator getXmlGenerator() {
		return xmlGenerator;
	}

	public void setXmlGenerator(SolrXmlDocGenerator xmlGenerator) {
		this.xmlGenerator = xmlGenerator;
	}

	public DataProvider getDataProvider() {
		return dataProvider;
	}

	public void setDataProvider(DataProvider dataProvider) {
		this.dataProvider = dataProvider;
	}

	public DataProcessor getDataProcessor() {
		return dataProcessor;
	}

	public void setDataProcessor(DataProcessor dataProcessor) {
		this.dataProcessor = dataProcessor;
	}

	public Map<String, DataBuffer> getDataBuffers() {
		return dataBuffers;
	}

	public void setDataBuffers(Map<String, DataBuffer> dataBuffers) {
		this.dataBuffers = dataBuffers;
	}

	public CapacityInfo getCapacityInfo() {
		return capacityInfo;
	}

	public void setCapacityInfo(CapacityInfo capacityInfo) {
		this.capacityInfo = capacityInfo;
	}

	public Map<String, IndexTransmitor> getIndexTramsmitors() {
		return indexTramsmitors;
	}

	public void setIndexTramsmitors(Map<String, IndexTransmitor> indexTramsmitors) {
		this.indexTramsmitors = indexTramsmitors;
	}

	public ServiceConfig getServiceConfig() {
		return serviceConfig;
	}

	public void setServiceConfig(ServiceConfig serviceConfig) {
		this.serviceConfig = serviceConfig;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public GroupRouter getRouter() {
		return router;
	}

	public void setRouter(GroupRouter router) {
		this.router = router;
	}

	public IndexType getIndexType() {
		return indexType;
	}

	public void setIndexType(IndexType indexType) {
		this.indexType = indexType;
	}

	public FetchDataExceptionHandler getFetchDataExceptionHandler() {
		return fetchDataExceptionHandler;
	}

	public void setFetchDataExceptionHandler(
			FetchDataExceptionHandler fetchDataExceptionHandler) {
		this.fetchDataExceptionHandler = fetchDataExceptionHandler;
	}

	public boolean getIsDumping() {
		return isDumping.get();
	}

	public boolean isInited() {
		return inited;
	}
}
