package com.taobao.terminator.core.index;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.taobao.terminator.core.util.IndexFileUtils;

/**
 * 增量索引job使用这个类来找到所有可识别的索引xml文件。<br>
 * 这个类会从一个指定记录文件中读取信息，这个文件中的信息指定了需要从哪个目录开始进行搜索。
 * 如果这个文件为空，或者不存在，那么就创建这个文件，从根目录开始搜索。否则的话就从记录文件
 * 中记录的开始点进行搜索。
 * 
 * 这个文件会找到所有可用的索引xml文件，然后将最后一个文件的名字和路径记录到记录文件中。这样
 * 下次就从这个位置开始搜索。
 * 
 * @author tianxiao
 *
 */
public class IncrIndexFileSearcher implements IndexFileSearcher {
	private static Log logger = LogFactory.getLog(IncrIndexFileSearcher.class);
	/**
	 * 开始搜索的根目录
	 */
	private File rootDir;
	/**
	 * 返回的最大文件数。默认为Long.MAX_VALUE
	 */
	private int maxReturnFileNum;
	
	public IncrIndexFileSearcher(File rootDir){
		this.maxReturnFileNum = IndexFileSearcher.DEFAULT_MAX_RETURN_FILE_NUM;
		this.rootDir = rootDir;
	}
	
	protected String getStartTime() throws IOException {
		return IndexFileUtils.getIncreStartDate(this.rootDir);
	}
	
	public Collection<File> listIndexFiles(Set<String> excludeFiles) {
		String startDate = "";
		try {
			startDate = this.getStartTime();
		} catch (IOException e) {
			logger.error("读取增量索引开始时间文件失败，取消本次增量。。", e);
			return null;
		}
		Collection<File> files = null;
		
		try {
			files = IndexFileUtils.listIncrFile(this.rootDir, startDate, this.maxReturnFileNum);
		} catch (ParseException e) {
			logger.error("解析增量开始文件名失败，请检查。。。", e);
		}
		
		if(files != null && files.size() != 0){
			Collections.sort((List<File>)files, new Comparator<File>(){
				public int compare(File arg0, File arg1) {
					return arg0.getName().compareTo(arg1.getName());
				}
			});
			
			String startFileName = "";
			startFileName = ((List<File>)files).get(files.size() - 1).getName();
			IndexFileUtils.writeIncrStartTimeToFile(this.rootDir, startFileName.substring(0, startFileName.lastIndexOf(".")));
		} 
		
 		return files;
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
