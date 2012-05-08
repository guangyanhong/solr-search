package com.taobao.terminator.core.realtime.commitlog2;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author yusen
 *
 */
public class SegmentPointAccessor {
	public static String fileName;
	
	protected File baseDir;
	protected File file;
	protected RandomAccessFile reader;
	protected RandomAccessFile writer;
	private boolean isNewCreate = false;
	
	static {
		fileName = "NONE.info";
	}
	
	public SegmentPointAccessor(File baseDir) throws IOException{
		this.init(baseDir);
	}
	
	private void init(File baseDir) throws IOException{
		this.baseDir = baseDir;
		this.file = new File(this.baseDir,fileName);
		if(!file.exists()) {
			this.isNewCreate = true;
		}
		
		this.writer = new RandomAccessFile(this.file, "rw");
		this.reader = new RandomAccessFile(this.file, "r");

		this.writer.seek(this.file.length());
	}
	
	public synchronized void write(SegmentPoint segmentPoint) throws IOException{
		long currentPoint = -1L;
		try {
			currentPoint = writer.getFilePointer();
			writer.writeLong(segmentPoint.getTime());
			writer.writeLong(segmentPoint.getSegmentNum());
			writer.writeLong(segmentPoint.getOffset());
		} catch (IOException e) {
			if(currentPoint != -1) {
				writer.seek(currentPoint);
			}
		}
	}
	
	public synchronized List<SegmentPoint> read() throws IOException{
		reader.seek(0L);
		List<SegmentPoint> list = new ArrayList<SegmentPoint>();
		SegmentPoint p = null;
		while((p = readOne()) != null) {
			list.add(p);
		}
		return list;
	}
	
	private SegmentPoint readOne() throws IOException {
		long currentPoint = -1L;
		try {
			currentPoint = reader.getFilePointer();
			long time = reader.readLong();
			long segmentNum = reader.readLong();
			long offset = reader.readLong();
			return new SegmentPoint(time,segmentNum, offset);
		} catch (IOException e) {
			if(currentPoint != -1) {
				reader.seek(currentPoint);
			}
			return null;
		}
	}
	
	public boolean isNewCreate() {
		return this.isNewCreate;
	}
	
	public SegmentPointAccessor reset() throws IOException {
		this.writer.setLength(0L);
		return this;
	}
	
	
	public static class FlushAtAcessor extends SegmentPointAccessor {
		static {
			fileName = "flushat.info";
		}
		
		public FlushAtAcessor(File baseDir) throws IOException {
			super(baseDir);
		}
	}
	
	public static class CheckPointAccessor extends SegmentPointAccessor {
		static {
			fileName = "checkpoint.info";
		}

		public CheckPointAccessor(File baseDir) throws IOException {
			super(baseDir);
		}
		
	}
	
	public static class FullAtAccessor extends SegmentPointAccessor {
		static {
			fileName = "fullat.info";
		}

		public FullAtAccessor(File baseDir) throws IOException {
			super(baseDir);
		}

		@Override
		public synchronized void write(SegmentPoint segmentPoint) throws IOException {
			try {
				writer.setLength(0L);
				writer.seek(0L);
				writer.writeLong(segmentPoint.getTime());
				writer.writeLong(segmentPoint.getSegmentNum());
				writer.writeLong(segmentPoint.getOffset());
			} catch (IOException e) {
				writer.seek(0L);
			}
		}
	}
	
	public static class SyncAtAccessor extends FullAtAccessor {
		static {
			fileName = "syncat.info";
		}

		public SyncAtAccessor(File baseDir) throws IOException {
			super(baseDir);
		}
	}
}
