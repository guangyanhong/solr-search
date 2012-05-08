package com.taobao.terminator.core.realtime.commitlog;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * CommitLog的片段文件
 * .
 * @author yusen
 */
public class CommitLogSegment implements Closeable {
	private RandomAccessFile reader;
	private RandomAccessFile writer;
	private RandomAccessFile headerAccessor;
	private File file = null;
	private Header header = null;
	volatile long currentReaderPoints ;
	volatile long currentWriterPoints ;

	public CommitLogSegment(File file) throws IOException {
		this.file = file;
		boolean newCreate = false;
		if (newCreate = !file.exists()) {
			file.createNewFile();
		}

		this.reader = new RandomAccessFile(file, "rw");
		this.writer = new RandomAccessFile(file, "rw");
		this.headerAccessor = new RandomAccessFile(file, "rw");
		this.headerAccessor.seek(0L);
		
		if (newCreate) {
			this.header = new Header();
			this.writer.write(this.header.toByteArray());
			this.currentWriterPoints = Header.length();
			this.currentReaderPoints = Header.length();
			this.writer.seek(Header.length());
			this.reader.seek(Header.length());
		} else {
			this.header = new Header(this.writer.readLong(), this.writer.readLong(), this.writer.readLong(), this.writer.readByte());
			this.writer.seek(header.lastWriteAt == Header.EMPTY_VALUE ? Header.length() : header.lastWriteAt);
			this.reader.seek(header.lastFlushAt == Header.EMPTY_VALUE ? Header.length() : header.lastFlushAt);
			this.currentWriterPoints = writer.getFilePointer();
			this.currentReaderPoints = reader.getFilePointer();
		}
	}

	public CommitLogSegment(File file, long readPos, long writePos) throws IOException {
		this(file);
		this.reader.seek(readPos);
		this.writer.seek(writePos);
	}
	
	public static Header readHeader(File file) throws IOException {
		if(!file.exists()) {
			throw new FileNotFoundException(file.getAbsolutePath());
		}
		
		DataInputStream dataInput = new DataInputStream(new FileInputStream(file));
		long lastFlushAt = dataInput.readLong();
		long lastWriteAt = dataInput.readLong();
		long fullAt = dataInput.readLong();
		byte isFinal = dataInput.readByte();
		
		return new Header(lastFlushAt, lastWriteAt, fullAt, isFinal);
	}

	void writerSeek(long pos) throws IOException {
		this.writer.seek(pos);
	}

	public void write(byte[] b) throws IOException {
		if (this.isFinal()) {
			throw new IOException("IS_FINAL");
		}

		if (b == null || b.length == 0) {
			return;
		}

		long currentPosition = -1L;
		try {
			currentPosition = writer.getFilePointer();
			Checksum checksum = new CRC32();
			writer.writeInt(b.length);
			writer.write(b);
			checksum.update(b, 0, b.length);
			writer.writeLong(checksum.getValue());
			currentWriterPoints = this.writer.getFilePointer();
			
			this.writeLastWriteAt();
		} catch (IOException e) {
			if (currentPosition != -1) {
				writer.seek(currentPosition);
			} 
			throw e;
		}
	}

	public byte[] read2() throws IOException {
		if(Header.length() == this.size()) {
			return null;
		}
		
		try {
			int size = reader.readInt();
			byte[] b = new byte[size];
			reader.readFully(b);

			Checksum checksum = new CRC32();
			checksum.update(b, 0, b.length);
			if (checksum.getValue() != reader.readLong()) {
				throw new IOException("read error,checksum");
			}
			
			this.currentReaderPoints = reader.getFilePointer();
			return b;
		} catch (EOFException e) {
			return null;
		}
	}
	
	public byte[] read() throws IOException {
		if(Header.length() == this.size()) {
			return null;
		}
		
		long currentPointer = reader.getFilePointer();
		int size = 0;
		try {
			
			/*int ch1 = reader.read();
			int ch2 = reader.read();
			int ch3 = reader.read();
			int ch4 = reader.read();
			if((ch1 | ch2 | ch3 | ch4) < 0) {
				reader.seek(currentPointer);
				return null;
			}*/
			
			size = reader.readInt();
			byte[] b = new byte[size];
			reader.readFully(b);

			Checksum checksum = new CRC32();
			checksum.update(b, 0, b.length);
			if (checksum.getValue() != reader.readLong()) {
				//throw new IOException("read error,checksum");
			}
			
			this.currentReaderPoints = reader.getFilePointer();
			return b;
		} catch (IOException e) {
			reader.seek(currentPointer);
			return null;
		} catch (OutOfMemoryError error) {
			reader.seek(currentPointer);
			System.out.println(size);
			
			return null;
		}
	}

	void readerSeek(long pos) throws IOException {
		this.reader.seek(pos);
	}

	public long size() throws IOException {
		return this.file.length();
	}

	public File getFile() {
		return this.file;
	}

	public Header getHeader() {
		return this.header;
	}

	public boolean isFinal() {
		return this.header.isFinal();
	}

	public boolean destroy() throws IOException {
		this.close();
		return this.file.delete();
	}

	@Override
	public void close() throws IOException {
		try {
			if (writer != null) {
				writer.close();
			}
			if (reader != null) {
				reader.close();
			}
			if(headerAccessor != null) {
				headerAccessor.close();
			}
		} finally {
			writer = null;
			reader = null;
		}
	}

	public long getReaderFilePointer() throws IOException {
		return this.reader.getFilePointer();
	}

	public long getWriterFilePointer() throws IOException {
		return this.writer.getFilePointer();
	}

	/**
	 * 读CommitLog的线程才会调用这个方法，为了不影响写线程，不与写线程争夺资源，此处用reader去做写的操作
	 * 
	 * @throws IOException
	 */
	public void writeLastFlushAt() throws IOException {
		this.header.lastFlushAt = this.getReaderFilePointer();
		writeHeader();
	}

	/**
	 * 写线程调用这个方法
	 * 
	 * @throws IOException
	 */
	void writeLastWriteAt() throws IOException {
		this.header.lastWriteAt = this.getWriterFilePointer();
		writeHeader();
	}

	/**
	 * 写线程调用     上层的CommitLog对象，当前的Segment大于等于预设的容量阀值的时候调用此方法
	 * 
	 * @throws IOException
	 */
	void writeIsFinal() throws IOException {
		this.header.isFinal = 1;
		writeHeader();
	}
	
	/**
	 * 全量Dump的线程调用此方法，记录全量开始点，便于全量过后的数据补足
	 * 
	 * @throws IOException
	 */
	public void writeFullAt() throws IOException {
		this.header.fullAt = this.getWriterFilePointer();
		writeHeader();
	}
	
	public void writeHeader() throws IOException {
		synchronized (header) { //多线程不能同时修改Header，同步一下
			headerAccessor.seek(0);
			headerAccessor.write(this.header.toByteArray());
		}
	}

	public static class Header {
		public static final long EMPTY_VALUE = -1L;

		public long lastFlushAt = EMPTY_VALUE;
		public long lastWriteAt = EMPTY_VALUE;
		public long fullAt = EMPTY_VALUE;
		public byte isFinal = 0;

		public Header() {
		}

		public Header(long lastFlushAt, long lastWriteAt, long fullAt, byte isFinal) {
			this.lastFlushAt = lastFlushAt;
			this.lastWriteAt = lastWriteAt;
			this.fullAt = fullAt;
			this.isFinal = isFinal;
		}

		public byte[] toByteArray() {
			byte[] b = new byte[25];
			b[0] = (byte) (lastFlushAt >>> 56);
			b[1] = (byte) (lastFlushAt >>> 48);
			b[2] = (byte) (lastFlushAt >>> 40);
			b[3] = (byte) (lastFlushAt >>> 32);
			b[4] = (byte) (lastFlushAt >>> 24);
			b[5] = (byte) (lastFlushAt >>> 16);
			b[6] = (byte) (lastFlushAt >>> 8);
			b[7] = (byte) (lastFlushAt >>> 0);

			b[8] = (byte) (lastWriteAt >>> 56);
			b[9] = (byte) (lastWriteAt >>> 48);
			b[10] = (byte) (lastWriteAt >>> 40);
			b[11] = (byte) (lastWriteAt >>> 32);
			b[12] = (byte) (lastWriteAt >>> 24);
			b[13] = (byte) (lastWriteAt >>> 16);
			b[14] = (byte) (lastWriteAt >>> 8);
			b[15] = (byte) (lastWriteAt >>> 0);

			b[16] = (byte) (fullAt >>> 56);
			b[17] = (byte) (fullAt >>> 48);
			b[18] = (byte) (fullAt >>> 40);
			b[19] = (byte) (fullAt >>> 32);
			b[20] = (byte) (fullAt >>> 24);
			b[21] = (byte) (fullAt >>> 16);
			b[22] = (byte) (fullAt >>> 8);
			b[23] = (byte) (fullAt >>> 0);

			b[24] = isFinal;
			return b;
		}

		public boolean isFinal() {
			return isFinal != 0;
		}

		public static int length() {
			return 25;
		}
	}
}
