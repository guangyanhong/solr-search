package com.taobao.terminator.core.realtime.commitlog2;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class CommitLogUtils {
	public static final String SEGMENT_PREFIX = "segment_";
	public static final String SEGMENT_EXTENSION = ".data";
	
	public static final byte IS_FINAL = 0;
	public static final byte IS_NOT_FINAL = 1;
	
	public static final long HEADER_LENGTH = 1L;
	
	public static boolean isFinalSegment(File file) {
		FileInputStream fin = null;
		try {
			fin = new FileInputStream(file);
			return fin.read() == IS_FINAL;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			/*if(fin != null) {
				try {
					fin.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}*/
		}
	}
	
	public static boolean isFinalSegment(RandomAccessFile fileAccessor)throws IOException {
		fileAccessor.seek(0);
		return fileAccessor.readByte() == IS_FINAL;
	}
	
	public static boolean deleteFile(File file) throws IOException{
		if(!file.exists()) {
			throw new FileNotFoundException(file.getAbsolutePath());
		}
		return file.delete();
	}
	
	public static File createNewSegmentFile(File baseDir) throws IOException {
		List<File> fileList = listSegmentFiles(baseDir);
		long currentNum = -1L;
		if(fileList != null && !fileList.isEmpty()) {
			File lastFile = fileList.get(fileList.size() - 1);
			currentNum = Long.valueOf(lastFile.getName().split("\\.")[0].split("_")[1]);
		}
		
		return new File(baseDir, genSegmentName(++currentNum));
	}
	
	public static File createNewSegmentFile(File baseDir,File currentFile) throws IOException {
		long currentNum = getSegmentNum(currentFile.getName());
		if(currentNum != -1) {
			return new File(baseDir,genSegmentName(++currentNum));
		} else {
			return createNewSegmentFile(baseDir);
		}
	}
	
	public static String genSegmentName(long num) {
		return new StringBuilder().append(SEGMENT_PREFIX).append(num).append(SEGMENT_EXTENSION).toString();
	}
	
	public static File nextFile(File baseDir,File currentFile) throws IOException {
		List<File> fileList = listSegmentFiles(baseDir);
		
		int i= 0;
		for(;i<fileList.size();i++) {
			File file = fileList.get(i);
			if(file.getName().equalsIgnoreCase(currentFile.getName())) {
				break;
			}
		}
		
		try {
			return fileList.get(i + 1);
		} catch (IndexOutOfBoundsException e) {
			return null;
		}
	}
	
	public static File getNextFile(File baseDir,File currentFile){
		List<File> list = listSegmentFiles(baseDir);
		int i = 0;
		for(;i<list.size();i++) {
			File file = list.get(i);
			if(getSegmentNum(file) > getSegmentNum(currentFile)) {
				return file;
			}
		}
		return null;
	}
	
	public static int compare(String fn1,String fn2) {
		long num1 = getSegmentNum(fn1);
		long num2 = getSegmentNum(fn2);

		if(num1 == num2) {
			return 0;
		} else {
			return num1 > num2 ? 1 : -1;
		}
	}
 	
	public static boolean hasNextFile(File baseDir,File currentFile) throws IOException {
		return getNextFile(baseDir, currentFile) != null;
	}

	public static List<File> listSegmentFiles(File baseDir) {
		if (!baseDir.exists()) {
			return null;
		}
		File[] segFiles = baseDir.listFiles(new CommitLogSegmentFilter());
		List<File> segFileList = sortSegmentFiles(segFiles);
		return segFileList;
	}
	
	public static List<File> listSegmentFiles(File baseDir,String endFileName,boolean includeEnd) throws IOException {
		if(!isSegment(endFileName)) {
			throw new IllegalArgumentException("SegmentName ==> " + endFileName);
		}
		
		List<File> fileList = listSegmentFiles(baseDir);
		int index = indexOf(fileList,endFileName);
		
		if(index < 0) {
			throw new FileNotFoundException("FileName ==> " + endFileName);
		}
		
		return includeEnd ? fileList.subList(0, index+1) : fileList.subList(0, index);
	}
	
	public static int indexOf(List<File> files,String fileName) {
		for(int i =0 ;i<files.size();i++) {
			if(files.get(i).getName().equals(fileName)) {
				return i;
			}
		}
		return -1;
	}

	public static List<File> sortSegmentFiles(File[] segFiles) {
		if (segFiles == null || segFiles.length == 0) {
			return null;
		}

		List<File> fileList = Arrays.asList(segFiles);
		Collections.sort(fileList, new CommitLogSegmentComparator());
		return fileList;
	}
	
	public static boolean isSegment(File file){
		return getSegmentNum(file) != -1L;
	}
	
	public static boolean isSegment(String fileName) {
		return getSegmentNum(fileName) != -1L;
	}
	
	public static long getSegmentNum(File file) {
		return getSegmentNum(file.getName());
	}
	
	public static long getSegmentNum(String fileName) {
		String[] parts = fileName.split("\\.");
		if(parts != null && parts.length == 2) {
			String[] p = parts[0].split("_");
			if(p != null && p.length == 2) {
				try {
					return Long.valueOf(p[1]);
				} catch (Exception e) {
					
				}
			}
		}
		return -1;
	}
	
	public static class CommitLogSegmentFilter implements FileFilter {
		private long minSuffix = Long.MIN_VALUE;
		private Set<String> exclusionNames = null;
		
		public CommitLogSegmentFilter() {}
		
		public CommitLogSegmentFilter(long minSuffix) {
			this(minSuffix,null);
		}
		
		public CommitLogSegmentFilter(long minSuffix,Set<String> exclusionNames) {
			this.minSuffix = minSuffix;
			this.exclusionNames = exclusionNames;
		}
		
		@Override
		public boolean accept(File file) {

			if (file.isFile()) {
				String name = file.getName();
				if(exclusionNames != null && exclusionNames.contains(name)) {
					return false;
				}
				
				if (name.startsWith("commitlog_") || name.endsWith(".data")) {
					String n = name.split("\\.")[0];
					String[] nameParts = n.split("_");
					if (nameParts.length == 2) {
						try {
							long suffix = Long.valueOf(nameParts[1]);
							return suffix >= minSuffix;
						} catch (NumberFormatException e) {
						}
					}
				}
			}
			return false;
		}
	}
	
	public static class CommitLogSegmentComparator implements Comparator<File> {

		@Override
		public int compare(File f1, File f2) {
			String[] nameParts1 = f1.getName().split("\\.")[0].split("_");
			String[] nameParts2 = f2.getName().split("\\.")[0].split("_");

			long suffix1 = Long.valueOf(nameParts1[1]);
			long suffix2 = Long.valueOf(nameParts2[1]);
			return suffix1 > suffix2 ? 1 : -1;
		}
	}
}
