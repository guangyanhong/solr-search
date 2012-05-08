package com.taobao.terminator.client.index.transmit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.client.index.IndexProvider;
import com.taobao.terminator.common.constant.IndexType;

public class IndexTransmitor4Test4LocalFile implements IndexTransmitor{
	private IndexType indexType = null;
	protected static Log logger = LogFactory.getLog(IndexProvider.class);
	
	private static final String basePath = IndexTransmitor4Test.class.getResource("/").getFile();
	private File fullDir = null;
	private File incrDir = null;
	
	public IndexTransmitor4Test4LocalFile(String serviceName,String groupName,IndexType indexType){
		this(serviceName + "-"  + groupName,indexType);
		fullDir = new File(basePath,"full");
		incrDir = new File(basePath,"incr");
		
		if(!fullDir.exists()){
			fullDir.mkdir();
		}else{
			try {
				FileUtils.cleanDirectory(fullDir);
			} catch (IOException e) {
				logger.error("清空本地Full目录失败",e);
			}
		}
		
		if(!incrDir.exists()){
			incrDir.mkdir();
		}else{
			try {
				FileUtils.cleanDirectory(incrDir);
			} catch (IOException e) {
				logger.error("清空本地Incr目录失败",e);
			}
		}
	}
	
	public IndexTransmitor4Test4LocalFile(String coreName,IndexType indexType){
		this.indexType = indexType;
	}
	
	@Override
	public boolean start() throws IndexTransmitException {
		return true;
	}

	@Override
	public boolean transmit(byte[] data) throws IndexTransmitException {
		String fileName = System.currentTimeMillis() + ".xml";
		File file = new File((indexType == IndexType.FULL? fullDir : incrDir) ,fileName);
		
		FileOutputStream fileOutputStream = null;
		try {
			fileOutputStream = new FileOutputStream(file);
			fileOutputStream.write(data);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			if(fileOutputStream != null)
				try {
					fileOutputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		return true;
	}
	
	@Override
	public boolean finish() throws IndexTransmitException {
		return true;
	}
}
