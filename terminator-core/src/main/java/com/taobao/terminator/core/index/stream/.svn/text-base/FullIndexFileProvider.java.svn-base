package com.taobao.terminator.core.index.stream;

import java.io.File;
import com.taobao.terminator.common.stream.FileProvider;

/**
 * 全量索引文件的提供者
 * 
 * @author yusen
 */
public class FullIndexFileProvider implements FileProvider{
	public static final String type = "fullIndexFiles";
	
	private File baseDir = null;
	
	public FullIndexFileProvider(File baseDir){
		if(baseDir == null)
			throw new IllegalArgumentException("baseDir can not be null!");
		this.baseDir = baseDir;
	}
	
	@Override
	public File getTargetFile(String name) {
		return new File(baseDir,name);
	}
}
