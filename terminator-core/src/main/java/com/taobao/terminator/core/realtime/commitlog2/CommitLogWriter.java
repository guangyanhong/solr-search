package com.taobao.terminator.core.realtime.commitlog2;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.taobao.terminator.core.realtime.commitlog2.Serializer.DefaultSerializer;

/**
 * 写CommitLog的对象，所有的操作全部做了同步保护
 * 
 * @author yusen
 */
public class CommitLogWriter {
	public static final long default_max_length = 100 * 1024 * 1024;
	private File baseDir;
	private File file;
	private RandomAccessFile fileAccessor;
	private Serializer serializer;
	private long maxLength = 100 * 1024 * 1024;
	
	/**
	 * 用默认的maxLength 和 默认的Serializer
	 * @param baseDir
	 * @throws IOException
	 */
	public CommitLogWriter(File baseDir) throws IOException {
		this(baseDir,-1,null);
	}
	
	/**
	 * 创建一个CommitLog的写操作对象，每次调用构造方法都会新创建一个文件作为当前的write文件，防止因为宕机导致的数据丢失
	 * 
	 * @param baseDir     目录
	 * @param maxLength   每个Segment的最大容量
	 * @param serializer  序列化方式
	 * 
	 * @throws IOException
	 */
	public CommitLogWriter(File baseDir,long maxLength,Serializer serializer) throws IOException{
		this.baseDir = baseDir;
		
		//将之前的文件全部置为IS_FINAL状态
		List<File> list = CommitLogUtils.listSegmentFiles(baseDir);
		if(list != null && !list.isEmpty()) {
			for(File file : list) {
				RandomAccessFile accessor = new RandomAccessFile(file, "rw");
				if(accessor.readByte() == CommitLogUtils.IS_NOT_FINAL) {
					accessor.seek(0L);
					accessor.writeByte(CommitLogUtils.IS_FINAL);
				}
				accessor.close();
			}
		}
		
		//每次new这个对象的时候都是新建一个File，避免上次宕机或者kill主机之后文件末尾数据不完整
		this.file = CommitLogUtils.createNewSegmentFile(baseDir);
		this.fileAccessor = new RandomAccessFile(this.file, "rw");
		this.fileAccessor.seek(0L);
		this.fileAccessor.writeByte(CommitLogUtils.IS_NOT_FINAL);//IS_NOT_FINAL
		
		if(maxLength >= 1024 * 1024) { //太小了就算了，忽略吧
			this.maxLength = maxLength;
		}
		
		if(serializer == null) {
			serializer = new DefaultSerializer();
		}
		this.serializer = serializer;
	}
	
	public synchronized void write(byte[] b) throws IOException {
		long currentPosition = -1L;
		try {
			currentPosition = fileAccessor.getFilePointer();
			Checksum checksum = new CRC32();
			fileAccessor.writeInt(b.length);
			fileAccessor.write(b);
			checksum.update(b, 0, b.length);
			fileAccessor.writeLong(checksum.getValue());
			
			this.mayBeCreateNew();
			
		} catch (IOException e) {
			if (currentPosition != -1) {
				fileAccessor.seek(currentPosition);
			} 
			throw e;
		}
	}
	
	public synchronized void write(Object obj) throws IOException {
		byte[] b = serializer.toByte(obj);
		this.write(b);
	}
	
	protected void mayBeCreateNew() throws IOException{
		if(file.length() >= this.maxLength) {
			this.createNew();
		}
	}
	
	protected void createNew() throws IOException{
		this.fileAccessor.seek(0L);
		this.fileAccessor.writeByte(CommitLogUtils.IS_FINAL);
		
		this.close();
		this.file = CommitLogUtils.createNewSegmentFile(baseDir,this.file);
		this.fileAccessor = new RandomAccessFile(file, "rw");
		this.fileAccessor.writeByte(CommitLogUtils.IS_NOT_FINAL);
	}
	
	public synchronized SegmentPoint getCurrentSegmentPoint() throws IOException {
		return new SegmentPoint(System.currentTimeMillis(), this.file.getName(), this.fileAccessor.getFilePointer());
	}
	
	public synchronized SegmentPoint getCurrentSegmentPoint(long currentTime) throws IOException {
		return new SegmentPoint(currentTime, this.file.getName(), this.fileAccessor.getFilePointer());
	}
	
	public synchronized void close() throws IOException {
		if(fileAccessor != null) {
			try {
				this.fileAccessor.close();
			} finally  {
				this.fileAccessor = null;
			}
		}
	}

	public File getBaseDir() {
		return baseDir;
	}

	public File getFile() {
		return file;
	}

	public RandomAccessFile getFileAccessor() {
		return fileAccessor;
	}

	public Serializer getSerializer() {
		return serializer;
	}

	public long getMaxLength() {
		return maxLength;
	}

	public void setMaxLength(long maxLength) {
		this.maxLength = maxLength;
	}
}
