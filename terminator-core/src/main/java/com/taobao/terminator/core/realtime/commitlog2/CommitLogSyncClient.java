package com.taobao.terminator.core.realtime.commitlog2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.protocol.Address;
import com.taobao.terminator.core.realtime.commitlog2.SyncUtils.NetException;

public class CommitLogSyncClient {
	private final Log log = LogFactory.getLog(CommitLogSyncClient.class);
	
	private File baseDir;
	private SyncInfo syncInfo;
	private Socket socket;
	private DataInputStream in;
	private DataOutputStream out;
	private long waitTime;
	private AddressFetcher addFetcher;
	private Thread syncThread;
	
	private LRUCache<String, RandomAccessFile> randomAccessFileCache = new LRUCache<String, RandomAccessFile>(5);
	private LRUCache<String, File> fileCache = new LRUCache<String, File>(5);
	
	public CommitLogSyncClient(File baseDir,AddressFetcher addFetcher,long waitTime) throws IOException {
		this.baseDir = baseDir;
		this.waitTime = waitTime;
		this.addFetcher = addFetcher;
		this.syncInfo = new SyncInfo(new File(baseDir,"sync.properties"), false);
	}
	
	public void start(){
		syncThread = new SyncThread();
		syncThread.start();
	}
	

	
	class SyncThread extends Thread {
		public SyncThread() {
			super("SYNC-COMMITLOG-CLIENT-THREAD");
		}
		
		public void run() {
			try {
				if(!connect()) {
					log.error("Connect Leader ERROR -- Server can not be started!!!");
					return;
				}
			} catch (IOException e) {
				log.error("Connect Leader ERROR!",e);
				return;
			}
			
			while(true) {
				try {
					doSync();
				} catch (IOException e) {
					if(e instanceof NetException) {  //Net ERROR reconnect to Leader
						if(e.getCause() instanceof EOFException || e.getCause() instanceof SocketException) { //对端Socket主动close(),读请求的时候才会抛出这个异常
							log.error("Leader ERROR,ServerSocket closed",e);
							
							try {
								if(!connect()) { //连接失败，Server端不能正常启动
									log.error("Re-Connect Leader ERROR -- Server can not be started!!!");
									return;
								}
							} catch (IOException e1) {
								log.error("Re-Connect Leader ERROR!",e1);
								return;
							}
						}
					} else {
						log.error("Sync ERROR!",e);
					}
				} catch (InterruptedException e) {
					log.error(e);
				} catch (ClassNotFoundException e) {
					log.error(e);
				}
			}
		}
	}

	/**
	 * 连接Server，如果Server没有启动(LeaderService调不通)，则一直尝试(间隔3s)，如果Leader的CommigLogSyncServer没有启动或者是启动过程中出现异常了，直接返回false
	 * 
	 * @throws IOException
	 */
	private boolean connect() throws IOException {
		Address add = null;
		try {
			log.warn("Reconnect to Leader...");
			add = this.addFetcher.fetch();
			if (add == null) { // Leader启动SyncCommitLog失败了
				log.error("Leader can not start CommitLogSyncServer!");
				return false;
			}
		} catch (Throwable e) { // 抛异常说明连接不上LeaderService，有可能是Leader机器宕机了,如果是宕机那么就每隔3s轮训，直到Leader活过来为止
			log.error("Can not connect to Leader!!Retry...");
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e1) {
			}

			return connect();
		}

		if (this.socket != null) {
			this.socket.close();
		}

		this.socket = new Socket();
		this.socket.connect(new InetSocketAddress(add.getIp(), add.getPort()));
		this.in = new DataInputStream(socket.getInputStream());
		this.out = new DataOutputStream(socket.getOutputStream());
		log.warn("Good,conected to Leader!!!");
		return true;
	}
	
	private void doSync() throws IOException,InterruptedException,ClassNotFoundException{
		SyncRequest request = new SyncRequest();
		
		String segmentName = syncInfo.getSegmentName();
		int offset = syncInfo.getOffset();
		
		if((segmentName == null || segmentName.trim().equals("")) && offset < 0) { //sync.properties文件是空的视为第一次同步，从头同步文件，本地还什么文件都没有
			request.setFirstSync(true);
			SyncUtils.writeObject(out, request);

			SyncResponse response = (SyncResponse)SyncUtils.readObject(in);
			String firstFileName = response.getFileName();
			if(firstFileName != null && !firstFileName.trim().equals("")) {
				syncInfo.setSegmentName(firstFileName);
				syncInfo.setOffset(0);
				syncInfo.store();
			}
			
		} else { //非第一次的情况
			
			request.setSegmentName(syncInfo.getSegmentName());
			request.setOffset(syncInfo.getOffset());
			
			SyncUtils.writeObject(out, request);
			
			SyncResponse response = (SyncResponse)SyncUtils.readObject(in);
			
			if(response.isOk()) {
				int len = response.getLength();
				if(response.isEOF()) {
					
					//同步完一个已经结束的文件，将这个文件的IS_FINAL位置为1，让Read的线程好决定是否读下去
					this.markToFinal(this.getFile(syncInfo.getSegmentName()));

					if(len == 0) {
						//读到了一个文件的末尾，而且下一个文件还没有创建,稍微休息会儿
						Thread.sleep(500);
					} else { 
						//读到一个文件的末尾，而且下一个文件已经存在了，创建这个文件开始同步数据
						String nextFileName = response.getFileName();
						File nextFile = this.createNewFile(nextFileName);
						this.readData(in, nextFile, 0, len);
						syncInfo.setSegmentName(nextFileName);
						syncInfo.setOffset((int)nextFile.length());
						syncInfo.store();
					}
					
				} else {
					if(len == 0) { //基本上两边同步了，休息一会儿，继续复制
						Thread.sleep(waitTime);
						log.warn("FULL_SYNC!!!!!!!!!!!!");
					} else {
						File file = this.getFile(syncInfo.getSegmentName());
						this.readData(in, file, syncInfo.getOffset(), len);
						syncInfo.setSegmentName(file.getName());
						syncInfo.setOffset((int)file.length());
						syncInfo.store();
					}
				}
			} else {
				//复制有问题，参数错误或者是别的什么
				log.error("Sync参数好像有问题!!");
			}
		}
	}
	
	private File getFile(String fileName) throws IOException{
		File file = fileCache.get(fileName);
		if(file == null) {
			file = new File(baseDir,fileName);
			fileCache.put(fileName, file);
		}
		return file;
	}

	
	private void readData(DataInputStream in, File targetFile, int offset, int len) throws IOException {

		RandomAccessFile accessFile = randomAccessFileCache.get(targetFile.getName());
		if (accessFile == null) {
			accessFile = new RandomAccessFile(targetFile, "rw");
			randomAccessFileCache.put(targetFile.getName(), accessFile);
		}

		accessFile.seek(offset);

		byte[] buff = new byte[1024 * 1024];

		final int chunkSize = buff.length;
		final int chunkNum = len / chunkSize;
		final int tailSize = len % chunkSize;

		for (int i = 0; i < chunkNum; i++) {
			in.readFully(buff);
			accessFile.write(buff, 0, chunkSize);
		}
		
		in.readFully(buff, 0, tailSize);
		accessFile.write(buff, 0, tailSize);
		accessFile.setLength(accessFile.getFilePointer());
	}

	private File createNewFile(String fileName) throws IOException{
		File file = new File(baseDir,fileName);
		if(file.exists()) {
			file.delete();
		}
		file.createNewFile();
		return file;
	}
	
	private void markToFinal(File file) throws IOException{
		RandomAccessFile accessFile = null;
		try {
			accessFile = new RandomAccessFile(file, "rw");
			accessFile.seek(0L);
			accessFile.write(CommitLogUtils.IS_FINAL);
		} finally {
			if(accessFile != null) {
				accessFile.close();
			}
		}
	}
	
	/**
	 * 获取Leader的Address，同步CommigLog文件
	 * 
	 * @author yusen
	 *
	 */
	public interface AddressFetcher {
		public Address fetch();
	}
	
	/**
	 * 获取固定的Leader的Address
	 * 
	 * @author yusen
	 *
	 */
	public static class FixedAddressFetcher implements AddressFetcher {
		private Address add;

		public FixedAddressFetcher(Address add) {
			this.add = add;
		}

		@Override
		public Address fetch() {
			return this.add;
		}
	}
	
	/**
	 * SynInfo保存为可读的文件，便于出现以外情况的人工干预
	 * 
	 * @author yusen
	 *
	 */
	public static class SyncInfo extends Properties {
		private static final long serialVersionUID = 1L;
		
		public static final String KEY_SEGMENT_NAME = "segmentNum";
		public static final String KEY_OFFSET = "offset";
		public static final String KEY_MASTER_DOWN_INFO = "masterDownInfo"; //保存宕机时刻Slave机器的CommitLog的信息，同步到那个Segment的哪儿（offset）了
		
		private File targetFile;
		private boolean autoStore = false;
		
		public SyncInfo(File targetFile, boolean autoStore) throws IOException {
			this.targetFile = targetFile;
			this.autoStore = autoStore;

			if (!targetFile.exists()) {
				targetFile.createNewFile();
			}

			FileInputStream fin = null;
			try {
				fin = new FileInputStream(this.targetFile);
				this.load(fin);
			} finally {
				if (fin != null) {
					fin.close();
				}
        }}
		
		public String getSegmentName() {
			return getProperty(KEY_SEGMENT_NAME);
		}
		
		public int getOffset() {
			String str = getProperty(KEY_OFFSET);
			
			if(str == null || str.trim().equals("")) {
				return Integer.MIN_VALUE;
			}
			
			return Integer.valueOf(str.trim());
		}
		
		public boolean setSegmentName(String name) {
			setProperty(KEY_SEGMENT_NAME, name);
			
			if(autoStore) {
				try {
					this.store();
					return true;
				} catch (IOException e) {
					
				}
			}
			return false;
		}
		
		public boolean setMasterDownLoadInfo(Pair p) {
			setProperty(KEY_MASTER_DOWN_INFO, Pair.toDesc(p));
			
			if(autoStore) {
				try {
					this.store();
					return true;
				} catch (IOException e) {
					
				}
			}
			return false;
		}
		
		public Pair getMasterDownInfo() {
			String desc = getProperty(KEY_MASTER_DOWN_INFO);
			return Pair.toPair(desc);
		}
		
		public boolean setOffset(int offset) {
			setProperty(KEY_OFFSET, String.valueOf(offset));

			if(autoStore) {
				try {
					this.store();
					return true;
				} catch (IOException e) {
				}
			}
			return false;
		}
		
		public void store() throws IOException{
			FileOutputStream fout = null;
	         try {
	            fout = new FileOutputStream(this.targetFile);
	            this.store(fout, null);
	         } finally {
	            if(fout != null) {
	               fout.close();
	            }
	         }
		}
		
		public static class Pair {
			public static final String splitChar = ":";
			
			public String segmentName;
			public int offset;
			
			public Pair(String segmentName, int offset) {
				super();
				this.segmentName = segmentName;
				this.offset = offset;
			}
			
			public String toString() {
				return new StringBuilder().append(segmentName).append(splitChar).append(offset).toString();
			}

			public static Pair toPair(String desc) {
				String[] parts = desc.split(splitChar);
				String segName = parts[0];
				String offsetStr = parts[1];
				int offset  = -1;
				
				if(!CommitLogUtils.isSegment(segName)) {
					throw new IllegalArgumentException("Format Error -- segmentName");
				}
				
				try {
					offset = Integer.valueOf(offsetStr);
				} catch (Exception e) {
					throw new IllegalArgumentException("Format Error -- offset", e);
				}
				
				return new Pair(segName, offset);
			}
			
			public static String toDesc(Pair p) {
				return p.toString();
			}
		}
	}
}
