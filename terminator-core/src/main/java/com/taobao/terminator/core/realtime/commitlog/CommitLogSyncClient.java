package com.taobao.terminator.core.realtime.commitlog;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.taobao.terminator.common.stream.Serializer;
import com.taobao.terminator.core.realtime.commitlog.CommitLogSegment.Header;

/**
 * CommitLog同步复制的Client端
 * 
 * @author yusen
 */
public class CommitLogSyncClient {
	private static Logger logger = Logger.getLogger(CommitLogSyncClient.class);
	
	private final int port;
	private final String host;
	private File baseDir;
	private FileProcessor processor;
	private boolean localIsEmpty = false;
	
	public CommitLogSyncClient(String host, int port,File baseDir,FileProcessor processor) {
		this.port = port;
		this.host = host;
		this.baseDir = baseDir;
		
		if(processor != null) {
			this.processor = processor;
		} else {
			this.processor = new DefaultFileProcessor();
		}
	}
	
	public CommitLogSyncClient(String host,int port,File baseDir) { 
		this(host,port,baseDir,null);
	}
	

	public void doSync() throws IOException {
		SocketChannel channel = SocketChannel.open();
		try {
			channel.socket().bind(new InetSocketAddress(InetAddress.getLocalHost(), 0));
			channel.connect(new InetSocketAddress(host, port));
			
			DataInputStream input = new DataInputStream(channel.socket().getInputStream());
			DataOutputStream output = new DataOutputStream(channel.socket().getOutputStream());
			
			List<File> segFiles = CommitLog.listSegmentFiles(baseDir);
			
			SyncReq req = null;
			if(segFiles == null || segFiles.isEmpty()) {
				req = new SyncReq("NONE");
				localIsEmpty = true;
			} else {
				File startFile = segFiles.get(segFiles.size() - 1);
				req = new SyncReq(startFile.getName());
				localIsEmpty = false;
			}
			
			{
				byte[] bytes = Serializer.objectToBytes(req);
				output.writeInt(bytes.length);
				output.write(bytes);
			}
			
			int size = input.readInt();
			byte[] contentBytes = new byte[size];
			input.readFully(contentBytes);
			
			SyncResp resp = (SyncResp) Serializer.bytesToObject(contentBytes);
			
			if(!resp.isSuc()){
				logger.error("Sync CommitLog Error ==> Request Server And Fetch File List From Server ==> " + resp.toString());
				return ;
			}
			
			List<FileInfo> fileInfos = resp.fileInfos;
			for(FileInfo fileInfo : fileInfos) {
				{
					byte[] bytes = Serializer.objectToBytes(fileInfo);
					output.writeBoolean(true);
					output.writeInt(bytes.length);
					output.write(bytes);
				}
				
				{
					File file = new File(baseDir,fileInfo.fileName);
					FileOutputStream fileOut = new FileOutputStream(file);
					
					byte[] data = new byte[1024*1024];
					int readNum = 0;
					int readedNum = 0;
					while(readedNum < fileInfo.size) {
						readNum = input.read(data);
						fileOut.write(data, 0, readNum);
						readedNum += readNum;
					}
					
					processor.process(file);
					try {
						fileOut.close();
					} finally {
						fileOut = null;
					}
				}
			}
			output.writeBoolean(false);//标记结束
		} finally {
			channel.close();
			localIsEmpty = false;
		}
	}
	
	public interface FileProcessor {
		public void process(File file) throws IOException;
	}
	
	public class DefaultFileProcessor implements FileProcessor {
		AtomicBoolean isFirstFile = new AtomicBoolean(true);
		@Override
		public void process(File file) throws IOException {
			if(!isFirstFile.getAndSet(false) || localIsEmpty) {
				CommitLogSegment seg = null;
				try {
					seg = new CommitLogSegment(file);
					seg.getHeader().lastFlushAt = (Header.EMPTY_VALUE);
					seg.writeHeader();
				} finally {
					if(seg != null) {
						seg.close();
					}
				}
			}
		}
	}
}
