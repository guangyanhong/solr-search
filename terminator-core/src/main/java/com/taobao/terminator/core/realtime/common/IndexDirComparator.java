package com.taobao.terminator.core.realtime.common;

import java.io.File;
import java.util.Comparator;

public class IndexDirComparator implements Comparator<File> {

	@Override
	public int compare(File f1, File f2) {
		String number1 = f1.getName().split("_")[1];
		String number2 = f1.getName().split("_")[1];
		
		int n1 = Integer.valueOf(number1);
		int n2 = Integer.valueOf(number2);
		
		return n1 - n2;
	}
}
