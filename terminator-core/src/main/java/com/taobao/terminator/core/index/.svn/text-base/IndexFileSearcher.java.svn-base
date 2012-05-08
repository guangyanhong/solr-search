package com.taobao.terminator.core.index;

import java.io.File;
import java.util.Collection;
import java.util.Set;

public interface IndexFileSearcher {
	public static final int DEFAULT_MAX_RETURN_FILE_NUM = 100;
	
	/**
	 * 返回xml格式的索引文件列表，不包括excludeFiles中的文件
	 * @param excludeFiles
	 * @return
	 * 	不会返回null，如果没有找到文件，将会返回一个大小为0的List
	 */
	public Collection<File> listIndexFiles(Set<String> excludeFiles);
	/**
	 * 获取搜索xml格式索引文件的根目录
	 * @return
	 */
	public File getRootDir();
	/**
	 * 设置搜索xml格式索引文件的根目录
	 * @param rootDir
	 */
	public void setRootDir(File rootDir);
	/**
	 * 设置最大的返回文件数
	 * @param num
	 */
	public void setMaxReturnFileNum(int num);
}
