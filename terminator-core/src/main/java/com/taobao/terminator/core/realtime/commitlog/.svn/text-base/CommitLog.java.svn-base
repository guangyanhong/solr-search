package com.taobao.terminator.core.realtime.commitlog;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.core.realtime.commitlog.CommitLogSegment.Header;

/**
 * 顺序记录所有的实时请求，实时的IndexBuilder通过不断的顺序读取该文件进行实时的索引构建<br>
 * CommitLog采用分Segment存储的方式，分Segment方式是为了便于管理，删除
 * 
 * @author yusen
 */
public class CommitLog implements Closeable{
	private final Log log = LogFactory.getLog(CommitLog.class);
	
	public static final  int DEFAULT_KEEP_DAYS = 2;
	public static final long DEFAULT_SIZE_OF_SEGMENT = 1024 * 1024 * 50;
	public static final String DEL_PENDING_FILE_NAME = "del_pendings.ter";

	private File baseDir;
	private long sizeOfSegment = 100 * 1024 * 1024;
	private List<CommitLogSegment> segments = null;
	volatile private int currentReaderIndex;
	volatile private int currentWriterIndex;
	private long currentFileSuffix = -1L;
	private Lock rewriteLock ,accessLock;
	
	public CommitLog(File baseDir) throws IOException{
		this(baseDir, DEFAULT_SIZE_OF_SEGMENT);
	}
	
	public CommitLog(File baseDir,long sizeOfSegment) throws IOException {
		if(!baseDir.exists()) {
			baseDir.mkdirs();
		}
		
		this.baseDir = baseDir;
		if(sizeOfSegment > 0) {
			this.sizeOfSegment = sizeOfSegment;
		}
		
		this.init();
		ReadWriteLock rwl = new ReentrantReadWriteLock();
		rewriteLock = rwl.writeLock();
		accessLock = rwl.readLock();
	}
	
	private Set<String> clearSegmentsAndGetDels() throws IOException{
		Set<String> delePendings = this.readDelPendings();
		if(delePendings != null && !delePendings.isEmpty()) {
			Set<String> remains = null;
			for(String name : delePendings) {
				File file = new File(baseDir,name);
				if(!file.delete()) {
					if(remains == null) {
						remains = new HashSet<String>();
					}
					remains.add(name);
				}
			}
			if(remains != null && !remains.isEmpty()) {
				this.writeDelPendings(remains,true);
			}
			return remains;
		}
		return null;
	}
	
	public static List<File> listSegmentFiles(File baseDir) throws IOException{
		if(!baseDir.exists()) {
			throw new FileNotFoundException("File Not Found ==> " + baseDir.getAbsolutePath());
		}
		File[] segFiles = baseDir.listFiles(new CommitLogSegmentFilter());
		List<File> segFileList = sortFiles(segFiles);
		return segFileList;
	}
	
	public static List<File> listSegmentFiles(File baseDir,Set<String> exclusionNames) throws IOException{
		if(!baseDir.exists()) {
			throw new FileNotFoundException("File Not Found ==> " + baseDir.getAbsolutePath());
		}
		File[] segFiles = baseDir.listFiles(new CommitLogSegmentFilter(Long.MIN_VALUE,exclusionNames));
		List<File> segFileList = sortFiles(segFiles);
		return segFileList;
	}
	
	void init() throws IOException {
		Set<String> dels = this.clearSegmentsAndGetDels();
		List<File> segFileList = listSegmentFiles(baseDir,dels);
		
		segments = new ArrayList<CommitLogSegment>();
		if(segFileList != null) {
			for(File segFile : segFileList) {
				segments.add(new CommitLogSegment(segFile));
			}
		} else {
			segments.add(this.createNew());
		}
		
		this.currentWriterIndex = segments.size() - 1;
		this.currentReaderIndex = this.getCurrentReaderSegment();
	}
	
	private int getCurrentReaderSegment() {
		if(segments.size() == 1) {
			return 0;
		} else {
			int i = segments.size() - 1;
			for (; i >= 0; i--) {
				long lastFlushAt = segments.get(i).getHeader().lastFlushAt;
				if(lastFlushAt != Header.EMPTY_VALUE && lastFlushAt >= Header.length()) {
					return i;
				}
			}
		}
		return 0;
	}

	/**
	 * 全量构建线程在全量完毕之后需要调用此方法，首先前清除FullAt之前的CommitLogSegment，让后让reader从FullAt点开始消费数据
	 * 
	 * @throws IOException
	 */
	public void clearAndSkipToFullAt() throws IOException {
		rewriteLock.lock();
		try {
			boolean found = false;
			long fullAt = Header.EMPTY_VALUE;
			Set<String> delePendings = null;
			//从右往左一次找，找到第一个含有FullAt的CommitLogSegment，这个之前的全部删除掉
			for (int i = segments.size() - 1; i >= 0; i--) {
				if (found) {
					try {
						segments.get(i).destroy();
					} catch (Exception e) {
						if(delePendings == null) {
							delePendings = new HashSet<String>();
						}
						delePendings.add(segments.get(i).getFile().getName());
					}
				} else {
					
					fullAt = segments.get(i).getHeader().fullAt;
					if (fullAt != Header.EMPTY_VALUE && fullAt >= Header.length()) {
						found = true;
					}
				}
			}
			
			if(found){
				List<File> segFileList = listSegmentFiles(baseDir,delePendings);
				
				segments.clear();
				for (File segFile : segFileList) {
					segments.add(new CommitLogSegment(segFile));
				}
				
				this.currentReaderIndex = 0;
				this.segments.get(0).readerSeek(fullAt);
			}
			
			if(delePendings != null && !delePendings.isEmpty()) {
				this.writeDelPendings(delePendings, false);
			}
		} finally {
			rewriteLock.unlock();
		}
	}
	
	private void writeDelPendings(Set<String> delePendings,boolean directCover) throws IOException{
		File file = new File(baseDir,DEL_PENDING_FILE_NAME);
		if(file.exists()) {
			if(directCover) {
				file.delete();
			} else {
				Set<String> oldPendings = this.readDelPendings();
				if(oldPendings != null && !oldPendings.isEmpty()) {
					for(String name : oldPendings) {
						delePendings.add(name);
					}
				}
			}
		} 
		
		DataOutputStream dataOut = null;
		try {
			file.createNewFile();
			FileOutputStream fileOut = new FileOutputStream(file);
			dataOut = new DataOutputStream(fileOut);
			for(String name : delePendings) {
				dataOut.writeUTF(name);
			}
		} finally {
			if(dataOut != null) {
				dataOut.close();
			}
		}
	}
	
	private Set<String> readDelPendings() throws IOException {
		File file = new File(baseDir,DEL_PENDING_FILE_NAME);
		if(!file.exists()) {
			return null;
		} else {
			Set<String> delePendings = new HashSet<String>();
			DataInputStream dataIn = null;
			try {
				dataIn = new DataInputStream(new FileInputStream(file));
				while (true) {
					String name = null;
					try {
						name = dataIn.readUTF();
					} catch (EOFException e) {
						break;
					}
					if(name != null) {
						delePendings.add(name);
					}
				}
			} finally {
				if(dataIn != null) {
					dataIn.close();
				}
			}
			return delePendings;
		}
	}
	
	/**
	 * 读线程调用此方法，当内存索引到达阀值的时候，内存索引Copy到磁盘上后新创建一个内存索引之前调用此方法，保证内存数据的恢复
	 * 全量线程在全量完毕之后也会调用此方法
	 * 
	 * @throws IOException
	 */
	public void writeLastFlushAt() throws IOException {
		accessLock.lock();
		try {
			segments.get(this.currentReaderIndex).writeLastFlushAt();
		} finally {
			accessLock.unlock();
		}
	}
	
	/**
	 * 全量的线程调用此方法，记录全量开始时间点上commitLog写入的位置，用于全量构建完毕后补偿全量期间的数据
	 * 
	 * @throws IOException
	 */
	public void writeFullAt() throws IOException {
		this.segments.get(this.currentWriterIndex).writeFullAt();
	}
	
	public void write(byte[] b) throws IOException {
		accessLock.lock();
		try {
			if(segments.get(this.currentWriterIndex).size() >= this.sizeOfSegment) {
				segments.get(this.currentWriterIndex).writeIsFinal();
				this.segments.add(this.createNew());
				this.currentWriterIndex = this.currentWriterIndex + 1;
			}
			
			segments.get(this.currentWriterIndex).write(b);
		} finally {
			accessLock.unlock();
		}
	}
	
	public byte[] read() throws IOException {
		accessLock.lock();
		try {
			byte[] b = segments.get(this.currentReaderIndex).read();
			if(b == null && segments.get(this.currentReaderIndex).isFinal() && this.segments.size() > this.currentReaderIndex + 1) {
				this.currentReaderIndex = this.currentReaderIndex + 1;
				return this.read();
			}
			
			return b;
		} finally {
			accessLock.unlock();
		}
	}
	
	public void writeObject(Object obj) throws IOException {
		ObjectOutputStream objOut = null;
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			objOut = new ObjectOutputStream(out);
			objOut.writeObject(obj);
			byte[] b = out.toByteArray();
			if(b.length > 1024 * 2024) {
				System.out.println(b);
			}
			this.write(b);
		} finally {
			if(objOut != null) {
				objOut.close();
			}
		}
	}
	
	public Object readObject() throws IOException, ClassNotFoundException {
		ObjectInputStream objIn = null;
		try {
			byte[] b = this.read();
			if(b == null) { 
				return null;
			}
			
			ByteArrayInputStream in = new ByteArrayInputStream(b);
			objIn = new ObjectInputStream(in);
			return objIn.readObject();
		} finally {
			if(objIn != null) {
				objIn.close();
			}
		}
	}
	
	private CommitLogSegment createNew() throws IOException {
		if(currentFileSuffix == -1) {
			List<File> fileList = listSegmentFiles(baseDir);
			if(fileList == null || fileList.isEmpty()) {
				currentFileSuffix = 0L;
			}else {
				File lastFile = fileList.get(fileList.size()-1);
				currentFileSuffix = Long.valueOf(lastFile.getName().split("\\.")[0].split("_")[1]);
			}
		}
		
		return new CommitLogSegment(new File(baseDir,"segment_" + (++currentFileSuffix) + ".data"));
	}
	
	public File getFile() {
		return this.baseDir;
	}
	
	public void clear(int days) throws IOException {
		rewriteLock.lock();
		try {
			long endTime = System.currentTimeMillis() - (days * 24 * 60 * 60 * 1000);
			
			int currentIndex = this.currentReaderIndex;
			int delCount = 0;
			boolean startToDel = false;
			for(int i = currentIndex - 1 ; i >= 0 ; i--) {
				CommitLogSegment seg = segments.get(i);
				long lastModified = seg.getFile().lastModified();
				if(lastModified <= endTime && !startToDel) {
					startToDel = true;
				}
				
				if(startToDel) {
					submitDelete(seg);
					delCount ++;
				}
			}
			
			List<File> segFileList = listSegmentFiles(baseDir);
			
			segments = new ArrayList<CommitLogSegment>();
			if(segFileList != null) {
				for(File segFile : segFileList) {
					segments.add(new CommitLogSegment(segFile));
				}
			} else {
				segments.add(this.createNew());
			}
			
			this.currentWriterIndex = segments.size() - 1;
			this.currentReaderIndex = currentIndex - delCount;
		} finally {
			rewriteLock.unlock();
		}
	}
	
	//TODO
	public void reload(boolean recordCurrentReadPoint) throws IOException {
		
	}
	
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	
	private void submitDelete(final CommitLogSegment segment) {
		executor.submit(new Runnable() {
			@Override
			public void run() {

				try {
					segment.destroy();
				} catch (Exception e) {
					throw new RuntimeException("Delete Segment",e);
				}
				
			}
		});
	}
	
	public void clear() throws IOException {
		this.clear(DEFAULT_KEEP_DAYS);
	}
	
	@Override
	public void close() throws IOException {
		for(CommitLogSegment seg : segments) {
			seg.close();
		}
	}
	
	private static List<File> sortFiles(File[] segFiles) {
		if(segFiles == null || segFiles.length == 0) {
			return null;
		}
			
		List<File> fileList = Arrays.asList(segFiles);
		Collections.sort(fileList, new CommitLogSegmentComparator());
		return fileList;
	}
	
	//startFileName = "NONE"时，从头取文件
	public List<FileInfo> getFileList(String startFileName,int length) throws IOException {
		int i = 0;
		boolean foundStart = false;
		List<FileInfo> fileList = new ArrayList<FileInfo>();
		for(CommitLogSegment seg : segments) {
			if(!foundStart && (seg.getFile().getName().equals(startFileName) || startFileName.equals("NONE"))) {
				foundStart = true;
			}
			
			if(foundStart) {
				i ++;
				fileList.add(new FileInfo(seg.getFile().getName(),seg.size()));
			}
			
			if(i == length) {
				break;
			}
		}
		
		if(!foundStart) {
			throw new FileNotFoundException("File Not Found ==> " + startFileName);
		}
		
		return fileList;
	}
}
