package com.taobao.terminator.core.util;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;

import com.taobao.terminator.common.constant.IndexEnum;

/**
 *  根据传入的文件名，找出在增量xml文件夹中所有比这个文件时间更新的xml文件。
 *  
 * @author tianxiao
 *
 */
public class IncrIndexFileFilter extends IndexFileFilter {
	private String startDate;
	private int count;
	
	/**
	 * @param fileName
	 */
	public IncrIndexFileFilter(String startDate, int numOfFiles){
		this.count = numOfFiles;
		this.startDate = startDate;
	}
	
	public boolean accept(File file) {
		int index = -1;
		String suffix = "";
		
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
			} 
			if(this.startDate.compareTo(file.getName().substring(0, file.getName().lastIndexOf("."))) < 0 ){
				count--;
				return true;
			} else{
				return false;
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
