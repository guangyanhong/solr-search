package com.taobao.terminator.core.index;

import java.io.File;
import java.util.Collection;
import java.util.Set;

import com.taobao.terminator.core.util.FullIndexFileFilter;
import com.taobao.terminator.core.util.IndexFileUtils;

/**
 * 全量索引时用来找文件类。<br>
 * 这个类扫描制定文件夹，然后将其中所有可以识别的索引xml文件找出来。
 * 全量索引的job会不断地调用这个类的listIndexFiles方法，知道全量索引job被通知停止。
 * @author tianxiao
 *
 */
public class FullIndexFileSearcher implements IndexFileSearcher {
	/**
	 * 搜索全量索引xml文件的跟目录
	 */
	private File rootDir;
	/**
	 * 返回的最大文件数。默认为Long.MAX_VALUE
	 */
	private int maxReturnFileNum;
	
	public Collection<File> listIndexFiles(Set<String> excludeFiles) {
		FullIndexFileFilter fileFilter = new FullIndexFileFilter(this.maxReturnFileNum, excludeFiles);
		
		return IndexFileUtils.listFile(this.rootDir, fileFilter, false);
	}

	public FullIndexFileSearcher(File rootDir){
		this.maxReturnFileNum = IndexFileSearcher.DEFAULT_MAX_RETURN_FILE_NUM;
		this.rootDir = rootDir;
	}

	public File getRootDir() {
		return this.rootDir;
	}

	public void setRootDir(File rootDir) {
		this.rootDir = rootDir;
	}
	
	public void setMaxReturnFileNum(int num){
		if(num > 0){
			this.maxReturnFileNum = num;
		}
	}
}
