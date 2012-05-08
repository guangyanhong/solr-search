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
 * ��������jobʹ����������ҵ����п�ʶ�������xml�ļ���<br>
 * �������һ��ָ����¼�ļ��ж�ȡ��Ϣ������ļ��е���Ϣָ������Ҫ���ĸ�Ŀ¼��ʼ����������
 * �������ļ�Ϊ�գ����߲����ڣ���ô�ʹ�������ļ����Ӹ�Ŀ¼��ʼ����������Ļ��ʹӼ�¼�ļ�
 * �м�¼�Ŀ�ʼ�����������
 * 
 * ����ļ����ҵ����п��õ�����xml�ļ���Ȼ�����һ���ļ������ֺ�·����¼����¼�ļ��С�����
 * �´ξʹ����λ�ÿ�ʼ������
 * 
 * @author tianxiao
 *
 */
public class IncrIndexFileSearcher implements IndexFileSearcher {
	private static Log logger = LogFactory.getLog(IncrIndexFileSearcher.class);
	/**
	 * ��ʼ�����ĸ�Ŀ¼
	 */
	private File rootDir;
	/**
	 * ���ص�����ļ�����Ĭ��ΪLong.MAX_VALUE
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
			logger.error("��ȡ����������ʼʱ���ļ�ʧ�ܣ�ȡ��������������", e);
			return null;
		}
		Collection<File> files = null;
		
		try {
			files = IndexFileUtils.listIncrFile(this.rootDir, startDate, this.maxReturnFileNum);
		} catch (ParseException e) {
			logger.error("����������ʼ�ļ���ʧ�ܣ����顣����", e);
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
