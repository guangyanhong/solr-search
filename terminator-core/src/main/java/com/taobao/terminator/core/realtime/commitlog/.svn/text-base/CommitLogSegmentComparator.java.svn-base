package com.taobao.terminator.core.realtime.commitlog;

import java.io.File;
import java.util.Comparator;

public  class CommitLogSegmentComparator implements Comparator<File> {

	@Override
	public int compare(File f1, File f2) {
		String[] nameParts1 = f1.getName().split("\\.")[0].split("_");
		String[] nameParts2 = f2.getName().split("\\.")[0].split("_");

		long suffix1 = Long.valueOf(nameParts1[1]);
		long suffix2 = Long.valueOf(nameParts2[1]);
		return suffix1 > suffix2 ? 1 : -1;
	}
}