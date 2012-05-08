package com.taobao.terminator.core.realtime.commitlog2;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.taobao.terminator.core.realtime.commitlog2.SegmentPointAccessor.CheckPointAccessor;
import com.taobao.terminator.core.realtime.commitlog2.SegmentPointAccessor.FlushAtAcessor;
import com.taobao.terminator.core.realtime.commitlog2.SegmentPointAccessor.FullAtAccessor;

/**
 * CommitLog最上层的封装访问类，继承了Writer，Reader和一些列的SegmentPointAccessor
 * 
 * @author yusen
 *
 */
public class CommitLogAccessor {
	
	private boolean isMaster = false;
	
	private File baseDir;
	private CommitLogWriter writer;
	private CommitLogReader reader;
	
	private SegmentPointAccessor checkPointAccessor;
	private SegmentPointAccessor flushAtAccessor;
	private SegmentPointAccessor fullAtAccessor;
	
	private long checkPointInterval = 30 * 60 * 1000;
	private long lastCheckPointTime = System.currentTimeMillis();
	
	/**
	 * 创建CommitLogAccessor对象
	 * 
	 * @param baseDir    CommitLog的存储目录
	 * @param maxLength  每个Segement的大小，如果小于0或者小于系同规定的最小值，则用默认的maxLength
	 * @param serializer 对象的序列话方式，如果为null，则默认为Java的序列化
	 * @param backNum    机器重启之后内存索引需要回复，该值表示了回退到那个点，如果参数太小则默认为1，如果太大，超过了FlushAt记录的总数，则从头开始回复
	 * @param mode       CommitLogAccessor分为Master模式和Slave模式，针对不同的服务器角色
	 * @throws IOException
	 */
	public CommitLogAccessor(File baseDir,long maxLength,Serializer serializer,int backNum,boolean isMaster) throws IOException{
		if(baseDir.exists() && baseDir.isFile()) {
			throw new IllegalArgumentException("The File ==> " + baseDir.getAbsolutePath() + "is a NOT a DIRECTORY!");
		}
		
		if(!baseDir.exists()) {
			baseDir.mkdirs();
		}
		
		this.baseDir = baseDir;
		this.isMaster = isMaster;
		
		this.flushAtAccessor = new FlushAtAcessor(baseDir);
		if(this.isMaster) {
			this.checkPointAccessor = new CheckPointAccessor(baseDir);
		}
		this.fullAtAccessor = new FullAtAccessor(baseDir);
		
		//初始化顺序不能改变
		if(this.isMaster()) {
			this.initWriter(maxLength, serializer);
		}
		this.initReader(serializer,backNum);
	}
	
	/**
	 * 创建Master模式的CommigLogAccessor模式<br>
	 * 
	 * Segment大小用默认的大小<br>
	 * Serializer用默认的Java序列化<br>
	 * 索引回复的回退步数为1
	 * 
	 * @param baseDir
	 * @return
	 * @throws IOException
	 */
	public static CommitLogAccessor createMasterAccessor(File baseDir) throws IOException{
		return new CommitLogAccessor(baseDir, -1, null, 1, true);
	}
	
	/**
	 * 创建Slave模式的CommigLogAccessor模式<br>
	 * 
	 * Segment大小用默认的大小<br>
	 * Serializer用默认的Java序列化<br>
	 * 索引回复的回退步数为1
	 * 
	 * @param baseDir
	 * @return
	 * @throws IOException
	 */
	public static CommitLogAccessor createSlaveAccessor(File baseDir) throws IOException {
		return new CommitLogAccessor(baseDir, -1, null, 1, false);
	}
	
	private void initWriter(long maxLength, Serializer serializer) throws IOException{
		this.writer = new CommitLogWriter(baseDir, maxLength, serializer);
		SegmentPoint p = this.writer.getCurrentSegmentPoint();
		this.lastCheckPointTime = p.getTime();
		checkPointAccessor.write(p);
	}
	
	private void initReader(Serializer serializer,int backNum) throws IOException {
		List<SegmentPoint> list = flushAtAccessor.read();
		SegmentPoint sp = null;
		if(!list.isEmpty()) {//正常情况下
			if(backNum <= 0 ) {
				backNum = 1;
			}
			
			int index = list.size() - backNum;
			if(index < 0) {
				index = 0;
			}
			
			sp = list.get(index);
		} else {
			//第一次启动或者flushat.info文件被删除或者被清空，此时没有了设定reader的起始位置的依据，直接从第一个文件的开头开始读，没有别的办法
			List<File> fileList = CommitLogUtils.listSegmentFiles(baseDir);
			if(fileList != null && !fileList.isEmpty()) {
				File firstFile = fileList.get(0);
				sp = new SegmentPoint(System.currentTimeMillis(), firstFile.getName(),CommitLogUtils.HEADER_LENGTH);
				
				this.flushAtAccessor.write(sp);
				this.fullAtAccessor.write(sp);
			} else { //Follower机器第一次启动还没有CommigLog的Data文件呢
				sp = null;
			}
		}
		
		this.reader = new CommitLogReader(baseDir, sp, serializer);
	}
	
	public boolean isMaster() {
		return this.isMaster;
	}
	
	public void ensureMasterMode() {
		if (!isMaster()) {
			throw new IllegalStateException("Is Not Master-Mode!");
		}
	}
	
	/**
	 * 写实时请求到CommitLog文件中，并做Chekcpoint的计数，每隔checkPointInterval时间周期记录一次CheckPoint
	 * 
	 * @param obj
	 * @throws IOException
	 */
	public void write(Object obj) throws IOException{
		this.ensureMasterMode();
		
		synchronized (writer) {
			long currentTime = System.currentTimeMillis();
			if(currentTime - lastCheckPointTime > checkPointInterval) {
				this.checkPointAccessor.write(this.getWriter().getCurrentSegmentPoint(currentTime));
				this.lastCheckPointTime = currentTime;
			}
			this.getWriter().write(obj);
		}
	}
	
	public Object read() throws IOException, InterruptedException, ClassNotFoundException{
		return this.getReader().readObject();
	}
	
	/**
	 * Read-Thread （也就是Index-Builder-Thread） 会调用此方法，当内存索引需要Flush到磁盘上的时候调用此方法记录Flush点，便于recover
	 * 
	 * @throws IOException
	 */
	public void writeFlushAt() throws IOException{
		synchronized (reader) {
			SegmentPoint sp = this.getReader().getCurrentSegmentPoint();
			this.getFlushAtAccessor().write(sp);
		}
	}
	
	/**
	 * Full-Dump-Thread（全量构建线程）在全量构建完成后调用此方法，记录全量后Index-Builder-Thread开始消费CommitLog的起始点
	 * 
	 * @param sp
	 * @throws IOException
	 */
	public void writeFullAt(SegmentPoint sp) throws IOException {
		this.getFullAtAccessor().write(sp);
	}

	/**
	 * 全量开始会有一个时间比如叫做fullStartTime，这个时间在全量完毕之后用于补全在全量期间的实时请求的数据
	 * 
	 * @param time  目标时间
	 * @param backNum 找到跟目标时间最接近的Point后往前回退多少
	 * @return
	 * @throws IOException
	 */
	public SegmentPoint getNearCheckPoint(long time,int backNum) throws IOException{
		List<SegmentPoint> list = this.getCheckPointAccessor().read();
		if(list == null || list.isEmpty()) {
			return null;
		}
		
		int i = 0;
		for(;i < list.size() ; i++) {
			SegmentPoint p = list.get(i);
			if(p.getTime() > time) {
				break;
			}
		}
		
		int index = i - backNum;
		if(index < 0) {
			index = 0;
		}
		
		return list.get(index);
	}
	
	public SegmentPoint getNearCheckPoint(long time) throws IOException {
		return this.getNearCheckPoint(time, 1);
	}
	
	/**
	 * 全量完毕后，Full-Dump-Thread调用此方法，进行SegmentFile的清理，和SemgntPoint的重置操作，并将CommitLogReader指向FullAt指向的文件
	 * @param time
	 * @throws IOException
	 */
	public void clearAndReset(long time) throws IOException{
		//回滚点记录作为fullAt点，并写文件
		SegmentPoint p = this.getNearCheckPoint(time);
		this.clearAndReset(p);
	}
	
	/**
	 * 清除掉p之前的SegmentFile文件，并且将Reader的指针指向p处
	 * @param p
	 */
	public void clearAndReset(SegmentPoint p) throws IOException{
		if(p != null) {
			this.getFullAtAccessor().reset().write(p);
			
			//fullAt点作为新的FlushAt-List的第一个元素
			this.getFlushAtAccessor().reset().write(p);
			
			//slave没有checkPoint
			if(this.isMaster) {
				//check-point也清空一下
				this.getCheckPointAccessor().reset().write(p);
			}
			
			//删除多余的Segment文件
			List<File> delList = CommitLogUtils.listSegmentFiles(baseDir, p.getSegmentName(), false);
			if(delList != null && !delList.isEmpty()) {
				for(File file : delList) {
					//TODO FIXME 以后可以开启任务删除
					CommitLogUtils.deleteFile(file);
				}
			}
			
			//释放掉老的Reader的资源 add by yusen 2011-03-13 17:16
			this.reader.close();
			
			//writer指向的File不变，改变reader指向的File
			CommitLogReader newReader = new CommitLogReader(this.baseDir,p);
			this.setReader(newReader);
		}
	}
	
	public CommitLogWriter getWriter() {
		this.ensureMasterMode();
		return writer;
	}

	private Object switchReaderLock = new Object();
	protected void setReader(CommitLogReader newReader) {
		synchronized (switchReaderLock) {
			this.reader = newReader;
		}
	}
	
	public CommitLogReader getReader() {
		synchronized (switchReaderLock) {
			return reader;
		}
	}

	public SegmentPointAccessor getCheckPointAccessor() {
		return checkPointAccessor;
	}

	public SegmentPointAccessor getFlushAtAccessor() {
		return flushAtAccessor;
	}

	public SegmentPointAccessor getFullAtAccessor() {
		return fullAtAccessor;
	}
	
	public static void mains(String[] args) throws Exception{
		final CommitLogAccessor accessor = new CommitLogAccessor(new File("C:\\CL"), 1024 * 1024, null,1,true);
		new Thread() {
			public void run() {
				for(int i = 0;i<12345;i++) {
					try {
						accessor.getWriter().write("Hello");
						Thread.sleep(10);
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}.start();
		
		new Thread() {
			public void run() {
				Object obj = null;
				int count = 1;
				try {
					while(true) {
						obj = accessor.getReader().readObject();
						if(obj != null) {
							System.out.println(obj + " " + (count++));
						} else {
							System.out.println("waiting....");
							Thread.sleep(5);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				} 
			}
		}.start();
	}
}
