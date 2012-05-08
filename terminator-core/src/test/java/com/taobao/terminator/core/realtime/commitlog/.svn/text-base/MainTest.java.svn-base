package com.taobao.terminator.core.realtime.commitlog;

import java.io.File;
import java.io.RandomAccessFile;

public class MainTest {
	public static void main(String[] args) throws Exception {
		RandomAccessFile r = new RandomAccessFile(new File("D:\\segment_1.data"), "rw");
		r.seek(1331388);
		System.out.println(r.readInt());
	}
}
