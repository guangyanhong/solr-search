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
	 * �жϴ��������Ƿ��ǺϷ��ģ��п�����Ram����fluahs�����̵Ĺ����г���崻������ʱ������flushing.sign�ļ�
	 * �������ļ����ڣ���ζ����������ļ�Ŀ¼������
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
	 * Ram������Flush������֮ǰ��������ļ����������Flush����
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
	 * Ram����Flush������֮��ɾ������ļ�����ʾ�Ѿ�Flush�ɹ�
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
	 * �г�dataĿ¼�µ����������ļ�Ŀ¼,����index,index_1,index_2,index_3....
	 * @param dataDir
	 * @return ���ݺ�׺�ź����File�б�
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
