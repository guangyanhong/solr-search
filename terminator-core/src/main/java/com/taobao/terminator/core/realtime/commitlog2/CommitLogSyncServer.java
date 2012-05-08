package com.taobao.terminator.core.realtime.commitlog2;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.protocol.Address;
import com.taobao.terminator.core.realtime.commitlog2.SegmentPointAccessor.CheckPointAccessor;
import com.taobao.terminator.core.realtime.commitlog2.SyncUtils.NetException;

public class CommitLogSyncServer {
	private final Log log = LogFactory.getLog(CommitLogSyncServer.class);
	private File baseDir;
	private String host;
	private int initPort;
	private int port;
	private int threadPoolSize = 10;
	private AcceptThread acceptThread ;
	
	public CommitLogSyncServer(File baseDir,String host,int initPort,int threadPoolSize) {
		this.baseDir = baseDir;
		this.host = host;
		this.initPort = initPort;
		this.threadPoolSize = threadPoolSize;
	}
	
	public Address getAdd() {
		return new Address(host, port);
	}
	
	public synchronized boolean isAlive(){
		return acceptThread != null && acceptThread.isAlive();
	}
	
	public synchronized int start() throws IOException{
		if(acceptThread != null) 
			throw new IllegalStateException("Already started!");
		
		ServerSocket serverSocket = new ServerSocket();
		
		int trialPort = initPort;
		
		SocketAddress add = new InetSocketAddress(this.host, trialPort);
		
		try {
			serverSocket.bind(add);
		} catch (IOException e) { //端口有冲突？顺序尝试100次，全部冲突那也没辙了
			
			int i = 0;
			for(i = 0 ;i<100 ;i++){
				try{
					serverSocket.bind(new InetSocketAddress(host, ++trialPort));
					break;
				}catch(IOException e1){
					if(i >= 99) {
						throw e1;
					}
				}
			}
		}
		
		acceptThread = new AcceptThread(serverSocket, this.threadPoolSize);
		acceptThread.start();

		return port = trialPort;
	}
	
	public synchronized void end() throws IOException {
		if(this.acceptThread == null) 
			throw new IllegalStateException("Already Closed!");
		this.acceptThread.close();
	}
	
	public int getPort() {
		return this.port;
	}
	
	public String getHost() {
		return this.host;
	}
	
	public class SyncJob implements Runnable {
		private Socket socket = null;
		private DataInputStream in;
		private DataOutputStream out;
		
		public SyncJob(Socket socket) throws IOException {
			this.socket = socket;
			this.in = new DataInputStream(socket.getInputStream());
			this.out = new DataOutputStream(socket.getOutputStream());
		}
		
		public void run() {
			while (true) {
				try {
					doRun();
				} catch (IOException e) {
					if (e instanceof NetException) {
						if (e.getCause() instanceof EOFException || e.getCause() instanceof SocketException) { // 对端Socket主动close(),读请求的时候才会抛出这个异常
							log.error("Net Error or Socket Error,close the socket!The client will be retry!", e);
							try {
								socket.close();
							} catch (IOException e1) {
								log.error("Close socket ERROR!", e);
							}
							break;
						}
					} else {
						log.error("Sync ERROR!", e);
					}

				} catch (ClassNotFoundException e) {
					log.error(e, e);
				}
			}
		}
		
		public void doRun() throws IOException,ClassNotFoundException{
			SyncRequest request = (SyncRequest)SyncUtils.readObject(in);
			SyncResponse response = new SyncResponse();
			
			//Slave第一次同步文件，Slave端没有任何文件，所以先问问Server，第一个文件的名字叫什么，接下来好从这个文件开始同步
			if(request.isFirstSync()) {
				File firstFile = CommitLogUtils.listSegmentFiles(baseDir).get(0);
				response.setFileName(firstFile.getName());
				SyncUtils.writeObject(out, response);
				
				return;
			}
			
			File file = this.findFile(request.getSegmentName());
			if(file == null) {
				response.setCode(SyncResponse.CODE_FILE_NOT_FOUND);
				SyncUtils.writeObject(out, response);
				
				return;
			}
			
			if(file.length() < request.getOffset() || request.getOffset() < 0) {
				response.setCode(SyncResponse.CODE_OFFSET_ERROR);
				SyncUtils.writeObject(out, response);
				
				return;
			}
			
			long endOffset = file.length();
			int dataLength = (int)(endOffset - request.getOffset());
			
			if(dataLength == 0) { //文件末尾 OR 同步的点和写入的同步了
				
				if(isFinal(file)) { //文件标记为IS_FINAL了
					File nextFile = this.findNextFile(file);
					
					response.setCode(SyncResponse.CODE_OK);
					response.setEOF(true);
					
					if(nextFile == null) { //下个文件还没有写呢
						
						response.setFileName(null);
						response.setLength(0);
						
						SyncUtils.writeObject(out, response);
						
						return;
						
					} else { //下个文件已经存在了
						
						response.setFileName(nextFile.getName());
						int l = (int)nextFile.length();
						response.setLength(l);
						
						SyncUtils.writeObject(out, response);
						
						this.writeData(out, nextFile, 0, l);
						
						return;
					}
					
				} else { //请求的位置和当前的位置一致
					response.setCode(SyncResponse.CODE_OK);
					response.setEOF(false);
					response.setLength(0);
					
					SyncUtils.writeObject(out, response);
					
					return;
				}
				
			} else { //一次很普通的同步，在文件的中部
				response.setCode(SyncResponse.CODE_OK);
				response.setEOF(false);
				response.setFileName(null);
				response.setLength(dataLength);
				
				SyncUtils.writeObject(out, response);
				
				this.writeData(out, file, request.getOffset(), dataLength);
				
				return;
			}
		}
		
		private LRUCache<String, RandomAccessFile> randomAccessFileCache = new LRUCache<String, RandomAccessFile>(10);
		
		private void writeData(DataOutputStream out,File srcFile,long offset,int len) throws IOException{
			try {
				RandomAccessFile accessor = randomAccessFileCache.get(srcFile.getName());
				if(accessor == null) {
					accessor = new RandomAccessFile(srcFile, "r");
					randomAccessFileCache.put(srcFile.getName(), accessor);
				}
				
				accessor.seek(offset);
				
				byte[] buff = new byte[1024 * 1024]; //1M buff
				
				
				final int chunkSize = buff.length;
				final int chunkNum = len/chunkSize;
				final int tailSize = len % chunkSize;
				
				for(int i = 0; i < chunkNum; i ++) {
					accessor.readFully(buff);
					out.write(buff, 0, chunkSize);
				}
				
				accessor.readFully(buff,0,tailSize);
				out.write(buff, 0, tailSize);
				
			} catch (Exception e) {
				try {
					out.close();
				} catch (IOException e1) {
					throw new RuntimeException(e);
				} finally {
					socket = null;
				}
			}
		}
		
		private LRUCache<String, File> fileCache = new LRUCache<String, File>(10);
		
		private File findFile(String fileName) {
			
			File file = fileCache.get(fileName);
			if(file == null) {
				file = new File(baseDir,fileName);
				if(file.exists()) {
					fileCache.put(file.getName(),file);
				}
			}
			
			if(file.exists()) {
				return file;
			} else {
				fileCache.remove(fileName);
			}
			
			return null;
		}
		
		private boolean isFinal(File file) {
			return CommitLogUtils.isFinalSegment(file);
		}
		
		private File findNextFile(File currentFile) {
			return CommitLogUtils.getNextFile(baseDir, currentFile);
		}
	}
	
	/**
	 * jiefdafadsfdfdas
	 * 
	 * @author yusen
	 *
	 */
	public class AcceptThread extends Thread implements Closeable{
		private ServerSocket serverSocket ;
		private ThreadPoolExecutor threadPool;
		
		public AcceptThread(ServerSocket serverSocket,int threadNum) {
			this.serverSocket = serverSocket;
			this.threadPool = new ThreadPoolExecutor(threadNum, threadNum,0L, TimeUnit.MILLISECONDS,new LinkedBlockingQueue<Runnable>());
			this.setName("CommitLog-Sync-Accept-Thread");
		}
		
		public void run() {
			while(true) {
				try {
					final Socket socket = serverSocket.accept();
					threadPool.execute(new SyncJob(socket));
				} catch (IOException e) {
					//忽略吧，继续
				}
			}
		}

		@Override
		public void close() throws IOException {
			if(serverSocket != null) {
				try {
					serverSocket.close();
				} finally {
					serverSocket = null;
				}
			}
		}
	}
}
