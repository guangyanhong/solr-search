package com.taobao.terminator.core.realtime.common;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;

public class IndexDirFilter implements FileFilter{
	@Override
	public boolean accept(File file) {
		if (file.isDirectory()) {
			String fileName = file.getName();
			String[] fileNameParts = fileName.split("_");

			if(fileNameParts.length == 2 && fileNameParts[0].equals("index")){
				try {
					Integer.valueOf(fileNameParts[1]);
					return file.listFiles(new FilenameFilter() {
						@Override
						public boolean accept(File dir, String name) {
							return name.equalsIgnoreCase(Utils.DISK_INDEX_SING_FILE_NAME);
						}
					}).length == 0;
				} catch (NumberFormatException e) {
					
				}
			}
		}
		return false;
	}
}
