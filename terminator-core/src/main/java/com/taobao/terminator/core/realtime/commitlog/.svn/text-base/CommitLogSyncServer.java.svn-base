package com.taobao.terminator.core.realtime.commitlog;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

import org.apache.log4j.Logger;

import com.taobao.terminator.common.stream.Serializer;

/**
 * CommitLog复制同步的Server端
 * 
 * @author yusen
 */
public class CommitLogSyncServer {
	private static Logger logger = Logger.getLogger(CommitLogSyncServer.class);
	
	private int port;
	private String host;
	private CommitLog commitLog;
	private AcceptThread acceptThread;
	
	public CommitLogSyncServer(String host,int port,CommitLog commitLog) {
		this.port = port;
		this.host = host;
		this.commitLog = commitLog;
	}
	
	public void setCommitLog(CommitLog commitLog) {
		this.commitLog = commitLog;
	}
	
	public boolean isAlive(){
		return acceptThread != null && acceptThread.isAlive();
	}
	
	public void start () throws IOException {
		if (acceptThread != null)
			throw new IllegalStateException("Already started");

		ServerSocketChannel channel = ServerSocketChannel.open();
		try{
			channel.socket().bind(new InetSocketAddress(host, port));
		}catch(IOException e){
			logger.warn("Socket.bind()");
			int i = 0;
			for(i = 0 ;i<100 ;i++){
				try{
					channel.socket().bind(new InetSocketAddress(host, ++port));
					logger.warn("绑定端口成功 ==>" + port);
					break;
				}catch(IOException e1){
					logger.warn("绑定端口失败  ==>" + port);
				}
			}
		}

		acceptThread = new AcceptThread(channel);
		acceptThread.setName("FileGetServer-Accept-Thread");
		acceptThread.start();
	}
	
	public int getPort() {
		return this.port;
	}
	
	protected class AcceptThread extends Thread {
		ServerSocketChannel channel = null;
		
		public AcceptThread(ServerSocketChannel channel) {
			this.channel = channel;
		}
		
		public void run() {
			while(true) {
				SocketChannel socketChannel = null;
				
				try {
					socketChannel = channel.accept();
					new ServerStreamThread(socketChannel).start(); //per request per thread
				} catch (AsynchronousCloseException e) {
					logger.warn("FileServer shutting down server thread.");
					break;
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
	
	protected class ServerStreamThread extends Thread {
		SocketChannel channel  = null;
		
		public ServerStreamThread(SocketChannel channel) {
			this.channel = channel;
		}
		
		public void run () {
			DataInputStream input = null; 
			DataOutputStream output = null; 
			try {
				input = new DataInputStream(channel.socket().getInputStream());;
				output = new DataOutputStream(channel.socket().getOutputStream());
				{
					int size = input.readInt();
					byte[] reqData = new byte[size];
					input.readFully(reqData);
					
					SyncReq req = (SyncReq)Serializer.bytesToObject(reqData);
					String startFileName = req.startFileName;
					
					if(startFileName == null || startFileName.trim().equals("")) {
						output.write(Serializer.objectToBytes(new SyncResp(SyncResp.REQ_ERROR, null)));
						return ;
					}
					
					List<FileInfo> fileInfos = commitLog.getFileList(startFileName, Integer.MAX_VALUE);
					
					if(fileInfos == null || fileInfos.isEmpty()) {
						output.write(Serializer.objectToBytes(new SyncResp(SyncResp.FILE_NOT_FOUND, null)));
						return ;
					}
					
					byte[] respData = Serializer.objectToBytes(new SyncResp(0, fileInfos));
					output.writeInt(respData.length);
					output.write(respData);
				}
				
				{
					while(input.readBoolean()) {
						int size = input.readInt();
						byte[] b = new byte[size];
						input.readFully(b);
						
						FileInfo fileInfo = (FileInfo)Serializer.bytesToObject(b);
						
						File segFile = new File(commitLog.getFile(),fileInfo.fileName);
						
						FileInputStream fileInputStream = new FileInputStream(segFile);
						
						try {
							byte data[]  = new byte[1024*1024*8];
							int  readnum = 0;
							while ((readnum = fileInputStream.read(data)) != -1) {
								ByteBuffer writeBuffer = ByteBuffer.wrap(data, 0, readnum);
								channel.write(writeBuffer);
							}
						} finally {
							fileInputStream.close();
						}
					}
				}

			} catch (IOException e) {
				logger.error(e,e);
			} finally {
				if(output != null) {
					try {
						output.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
