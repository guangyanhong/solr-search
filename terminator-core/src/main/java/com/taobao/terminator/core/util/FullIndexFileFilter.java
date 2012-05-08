package com.taobao.terminator.core.util;

import java.io.File;
import java.util.Set;

import com.taobao.terminator.common.constant.IndexEnum;

/**
 * 用来过滤索引原始数据文件，后缀名必须是tidx，同时返回指定数量的文件。
 * <code>IndexFileFilter(int numOfFiles)</code>构造函数用来指定<br>
 * 最多返回的文件数，超过的文件将会被过滤掉。
 * @author tianxiao
 *
 */
public class FullIndexFileFilter extends IndexFileFilter {
	private static final int DEFAULT_NUM_OF_FILES = 0;
	private int count;
	private Set<String> excludeFiles;
	
	public FullIndexFileFilter(int numOfFiles, Set<String> excludeFiles){
		if(numOfFiles <= 0){
			this.count = DEFAULT_NUM_OF_FILES;
		} else{
			this.count = numOfFiles;
		}
		this.excludeFiles = excludeFiles;
	}
	
	/**
	 * 获取numOfFiles个文件，超过的文件被过滤掉
	 */
	public boolean accept(File file) {
		int index = -1;
		String suffix = "";
		if(this.excludeFiles != null && this.excludeFiles.contains(file.getName())){
			return false;
		}
		
		if(file.isDirectory()){
			return true;
		}
		
		if(count > 0){
			String fileName = file.getName();
			index = fileName.lastIndexOf(".");
			if(index == -1){
				return false;
			} else{
				suffix = fileName.substring(index);
			}
			if(!suffix.contains(IndexEnum.INDEX_FILE_SUFFIX.getValue())){
				return false;
			} else{ 
				count--;
				return true;
			}
		} else{
			return false;
		}
	}
	
	public void resetCount(){
		this.count = 0;
	}
	
	public void setMaxNumOfFiles(int numOfFiles){
		this.count = numOfFiles;
	}
}
