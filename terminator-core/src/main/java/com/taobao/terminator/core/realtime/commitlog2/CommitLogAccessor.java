package com.taobao.terminator.core.realtime.commitlog2;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.taobao.terminator.core.realtime.commitlog2.SegmentPointAccessor.CheckPointAccessor;
import com.taobao.terminator.core.realtime.commitlog2.SegmentPointAccessor.FlushAtAcessor;
import com.taobao.terminator.core.realtime.commitlog2.SegmentPointAccessor.FullAtAccessor;

/**
 * CommitLog���ϲ�ķ�װ�����࣬�̳���Writer��Reader��һЩ�е�SegmentPointAccessor
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
	 * ����CommitLogAccessor����
	 * 
	 * @param baseDir    CommitLog�Ĵ洢Ŀ¼
	 * @param maxLength  ÿ��Segement�Ĵ�С�����С��0����С��ϵͬ�涨����Сֵ������Ĭ�ϵ�maxLength
	 * @param serializer ��������л���ʽ�����Ϊnull����Ĭ��ΪJava�����л�
	 * @param backNum    ��������֮���ڴ�������Ҫ�ظ�����ֵ��ʾ�˻��˵��Ǹ��㣬�������̫С��Ĭ��Ϊ1�����̫�󣬳�����FlushAt��¼�����������ͷ��ʼ�ظ�
	 * @param mode       CommitLogAccessor��ΪMasterģʽ��Slaveģʽ����Բ�ͬ�ķ�������ɫ
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
		
		//��ʼ��˳���ܸı�
		if(this.isMaster()) {
			this.initWriter(maxLength, serializer);
		}
		this.initReader(serializer,backNum);
	}
	
	/**
	 * ����Masterģʽ��CommigLogAccessorģʽ<br>
	 * 
	 * Segment��С��Ĭ�ϵĴ�С<br>
	 * Serializer��Ĭ�ϵ�Java���л�<br>
	 * �����ظ��Ļ��˲���Ϊ1
	 * 
	 * @param baseDir
	 * @return
	 * @throws IOException
	 */
	public static CommitLogAccessor createMasterAccessor(File baseDir) throws IOException{
		return new CommitLogAccessor(baseDir, -1, null, 1, true);
	}
	
	/**
	 * ����Slaveģʽ��CommigLogAccessorģʽ<br>
	 * 
	 * Segment��С��Ĭ�ϵĴ�С<br>
	 * Serializer��Ĭ�ϵ�Java���л�<br>
	 * �����ظ��Ļ��˲���Ϊ1
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
		if(!list.isEmpty()) {//���������
			if(backNum <= 0 ) {
				backNum = 1;
			}
			
			int index = list.size() - backNum;
			if(index < 0) {
				index = 0;
			}
			
			sp = list.get(index);
		} else {
			//��һ����������flushat.info�ļ���ɾ�����߱���գ���ʱû�����趨reader����ʼλ�õ����ݣ�ֱ�Ӵӵ�һ���ļ��Ŀ�ͷ��ʼ����û�б�İ취
			List<File> fileList = CommitLogUtils.listSegmentFiles(baseDir);
			if(fileList != null && !fileList.isEmpty()) {
				File firstFile = fileList.get(0);
				sp = new SegmentPoint(System.currentTimeMillis(), firstFile.getName(),CommitLogUtils.HEADER_LENGTH);
				
				this.flushAtAccessor.write(sp);
				this.fullAtAccessor.write(sp);
			} else { //Follower������һ��������û��CommigLog��Data�ļ���
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
	 * дʵʱ����CommitLog�ļ��У�����Chekcpoint�ļ�����ÿ��checkPointIntervalʱ�����ڼ�¼һ��CheckPoint
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
	 * Read-Thread ��Ҳ����Index-Builder-Thread�� ����ô˷��������ڴ�������ҪFlush�������ϵ�ʱ����ô˷�����¼Flush�㣬����recover
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
	 * Full-Dump-Thread��ȫ�������̣߳���ȫ��������ɺ���ô˷�������¼ȫ����Index-Builder-Thread��ʼ����CommitLog����ʼ��
	 * 
	 * @param sp
	 * @throws IOException
	 */
	public void writeFullAt(SegmentPoint sp) throws IOException {
		this.getFullAtAccessor().write(sp);
	}

	/**
	 * ȫ����ʼ����һ��ʱ��������fullStartTime�����ʱ����ȫ�����֮�����ڲ�ȫ��ȫ���ڼ��ʵʱ���������
	 * 
	 * @param time  Ŀ��ʱ��
	 * @param backNum �ҵ���Ŀ��ʱ����ӽ���Point����ǰ���˶���
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
	 * ȫ����Ϻ�Full-Dump-Thread���ô˷���������SegmentFile��������SemgntPoint�����ò���������CommitLogReaderָ��FullAtָ����ļ�
	 * @param time
	 * @throws IOException
	 */
	public void clearAndReset(long time) throws IOException{
		//�ع����¼��ΪfullAt�㣬��д�ļ�
		SegmentPoint p = this.getNearCheckPoint(time);
		this.clearAndReset(p);
	}
	
	/**
	 * �����p֮ǰ��SegmentFile�ļ������ҽ�Reader��ָ��ָ��p��
	 * @param p
	 */
	public void clearAndReset(SegmentPoint p) throws IOException{
		if(p != null) {
			this.getFullAtAccessor().reset().write(p);
			
			//fullAt����Ϊ�µ�FlushAt-List�ĵ�һ��Ԫ��
			this.getFlushAtAccessor().reset().write(p);
			
			//slaveû��checkPoint
			if(this.isMaster) {
				//check-pointҲ���һ��
				this.getCheckPointAccessor().reset().write(p);
			}
			
			//ɾ�������Segment�ļ�
			List<File> delList = CommitLogUtils.listSegmentFiles(baseDir, p.getSegmentName(), false);
			if(delList != null && !delList.isEmpty()) {
				for(File file : delList) {
					//TODO FIXME �Ժ���Կ�������ɾ��
					CommitLogUtils.deleteFile(file);
				}
			}
			
			//�ͷŵ��ϵ�Reader����Դ add by yusen 2011-03-13 17:16
			this.reader.close();
			
			//writerָ���File���䣬�ı�readerָ���File
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
