package com.taobao.terminator.core.index;

import java.io.File;
import java.util.Collection;
import java.util.Set;

import com.taobao.terminator.core.util.FullIndexFileFilter;
import com.taobao.terminator.core.util.IndexFileUtils;

/**
 * ȫ������ʱ�������ļ��ࡣ<br>
 * �����ɨ���ƶ��ļ��У�Ȼ���������п���ʶ�������xml�ļ��ҳ�����
 * ȫ��������job�᲻�ϵص���������listIndexFiles������֪��ȫ������job��ֹ֪ͨͣ��
 * @author tianxiao
 *
 */
public class FullIndexFileSearcher implements IndexFileSearcher {
	/**
	 * ����ȫ������xml�ļ��ĸ�Ŀ¼
	 */
	private File rootDir;
	/**
	 * ���ص�����ļ�����Ĭ��ΪLong.MAX_VALUE
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
