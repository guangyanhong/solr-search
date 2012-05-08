package com.taobao.terminator.core.realtime.commitlog;

import java.io.File;
import java.io.FileFilter;
import java.util.Set;

public class CommitLogSegmentFilter implements FileFilter {
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
