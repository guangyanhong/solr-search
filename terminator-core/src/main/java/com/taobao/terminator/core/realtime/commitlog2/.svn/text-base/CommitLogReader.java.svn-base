package com.taobao.terminator.core.realtime.commitlog2;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.taobao.terminator.core.realtime.commitlog2.Serializer.DefaultSerializer;

public class CommitLogReader {
	private File baseDir;
	private File file;
	private Serializer serializer;
	private RandomAccessFile fileAccessor;
	
	public CommitLogReader(File baseDir,SegmentPoint segmentInfo) throws IOException {
		this.init(baseDir, segmentInfo,null);
	}
	
	public CommitLogReader(File baseDir,SegmentPoint segmentInfo,Serializer serializer) throws IOException {
		this.init(baseDir, segmentInfo,serializer);
	}
	
	private void init(File baseDir,SegmentPoint segmentInfo,Serializer serializer)  throws IOException {
		this.baseDir = baseDir;
		this.serializer = serializer;
		if(this.serializer == null) {
			this.serializer = new DefaultSerializer();
		}
		
		if(segmentInfo != null) { //segmentInfo为空的情况：系统第一次启动，还没有Commitlog的文件呢
			this.file = new File(this.baseDir,segmentInfo.getSegmentName());
			this.fileAccessor = new RandomAccessFile(this.file,"r");
			this.fileAccessor.seek(segmentInfo.getOffset());
		}
	}
	
	public synchronized byte[] read() throws IOException,InterruptedException{
		
		if(file == null) { //系统第一次启动，还没有CommitLog数据文件存在，监听baseDir下的文件，如果有了，就可以从头读了
			List<File> files = CommitLogUtils.listSegmentFiles(baseDir);
			if(files != null && !files.isEmpty()) {
				file = files.get(0);
				fileAccessor = new RandomAccessFile(file,"r");
				fileAccessor.seek(CommitLogUtils.HEADER_LENGTH);
			} else {
				Thread.sleep(5000);
			}
			return null;
		}
		
		long currentPointer = -1L;
		try {
			currentPointer = fileAccessor.getFilePointer();
			
			int size = fileAccessor.readInt();
			byte[] b = new byte[size];
			fileAccessor.readFully(b);

			Checksum checksum = new CRC32();
			checksum.update(b, 0, b.length);
			if (checksum.getValue() != fileAccessor.readLong()) {
				throw new IOException("read error,checksum");
			}
			
			return b;
		} catch (IOException e) {
			if(this.isFinal()) {
				if(this.moveToNextFile()) {
					return this.read();
				}
			} else {
				if(currentPointer != -1) {
					fileAccessor.seek(currentPointer);
				}
			}
			return null;
		}
	}
	
	public synchronized Object readObject() throws IOException, InterruptedException, ClassNotFoundException {
		byte[] b = this.read();
		return b != null ? serializer.toObject(b) : null;
	}
	
	public synchronized SegmentPoint getCurrentSegmentPoint() throws IOException{
		return new SegmentPoint(System.currentTimeMillis(), this.file.getName(), this.fileAccessor.getFilePointer());
	}
	
	public synchronized SegmentPoint getCurrentSegmentPoint(long time) throws IOException{
		return new SegmentPoint(time, this.file.getName(), this.fileAccessor.getFilePointer());
	}
	
	protected boolean isFinal() throws IOException{
		this.fileAccessor.seek(0);
		return fileAccessor.readByte() == CommitLogUtils.IS_FINAL;
	}
	
	protected boolean moveToNextFile() throws IOException{
		boolean hasNext = false;
		File nextFile = CommitLogUtils.getNextFile(baseDir, file);
		if(nextFile != null) {
			this.close();
			this.file = nextFile;
			this.fileAccessor = new RandomAccessFile(this.file, "r");
			this.fileAccessor.seek(CommitLogUtils.HEADER_LENGTH);
			hasNext = true;
		}
		return hasNext;
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
	
	public Serializer getSerializer() {
		return serializer;
	}

	public void setSerializer(Serializer serializer) {
		this.serializer = serializer;
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
}	
