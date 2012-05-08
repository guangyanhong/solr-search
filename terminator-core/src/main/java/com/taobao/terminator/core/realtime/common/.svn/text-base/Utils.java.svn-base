package com.taobao.terminator.core.realtime.common;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Utils {
	public static final String DISK_INDEX_SING_FILE_NAME = "flushing.sign";
	
	/**
	 * 判断磁盘索引是否是合法的，有可能在Ram索引fluahs到磁盘的过程中出现宕机，这个时候会存在flushing.sign文件
	 * 如果这个文件存在，意味着这个索引文件目录有问题
	 * @param indexDir
	 * @return
	 */
	public static boolean isProperIndex(File indexDir) {
		File[] fl = indexDir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File file) {
				if(file.getName().equals(DISK_INDEX_SING_FILE_NAME)) {
					return true;
				}
				return false;
			}
		});
		return fl == null || fl.length == 0;
	}
	
	/**
	 * Ram的索引Flush到磁盘之前创建标记文件，标记正在Flush索引
	 * 
	 * @param dir
	 * @return
	 * @throws IOException
	 */
	public static boolean createSignFile(File dir) throws IOException {
		File file = new File(dir,DISK_INDEX_SING_FILE_NAME);
		if(!file.exists()) {
			file.createNewFile();
		}
		return true;
	}
	
	/**
	 * Ram索引Flush到磁盘之后删除标记文件，表示已经Flush成功
	 * 
	 * @param dir
	 * @return
	 */
	public static boolean deleteSignFile(File dir) {
		File file = new File(dir,DISK_INDEX_SING_FILE_NAME);
		if(file.exists()) {
			return file.delete();
		}
		return true;
	}
	
	public static boolean isNumber(String numberstr) {
		for(int i = 0; i< numberstr.length();i++) {
			if(!Character.isDigit(numberstr.charAt(i))){
				return false;
			}
		}
		return true;
	}
	
	/**
	 * 列出data目录下的所有索引文件目录,比如index,index_1,index_2,index_3....
	 * @param dataDir
	 * @return 根据后缀排好序的File列表
	 */
	public static List<File> listIndexDirs(File dataDir) {
		return sortIndexDirs(dataDir.listFiles(new IndexDirFilter()));
	}
	
	public static List<File> sortIndexDirs(File[] indexDirs) {
		List<File> files = Arrays.asList(indexDirs);
		Collections.sort(files, new IndexDirComparator());
		return files;
	}
}
