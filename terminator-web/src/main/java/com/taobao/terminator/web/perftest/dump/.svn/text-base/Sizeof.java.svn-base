package com.taobao.terminator.web.perftest.dump;

public class Sizeof {
	public static void main(String[] args) throws Exception {
		runGC();
		long heap1 = usedMemory();
/*		final int count = 6000000;
		byte[][] objects = new byte[count][];

		long heap1 = 0;
		for (int i = -1; i < count; ++i) {
			byte[] object = new byte[4];

			if (i >= 0)
				objects[i] = object;
			else {
				object = null; 
				runGC();
				heap1 = usedMemory(); 
			}
		}*/
		
		final int count = 60000000;
		byte[] is = new byte[count*4];
		
		runGC();
		long heap2 = usedMemory();

		System.out.println("'before' heap: " + heap1 + ", 'after' heap: " + heap2);
		System.out.println("heap delta: " + (heap2 - heap1)/(1024*1024));
	}

	private static void runGC() throws Exception {
		for (int r = 0; r < 4; ++r)
			_runGC();
	}

	private static void _runGC() throws Exception {
		long usedMem1 = usedMemory(), usedMem2 = Long.MAX_VALUE;
		for (int i = 0; (usedMem1 < usedMem2) && (i < 500); ++i) {
			s_runtime.runFinalization();
			s_runtime.gc();
			Thread.currentThread().yield();
			usedMem2 = usedMem1;
			usedMem1 = usedMemory();
		}
	}

	private static long usedMemory() {
		return s_runtime.totalMemory() - s_runtime.freeMemory();
	}

	private static final Runtime s_runtime = Runtime.getRuntime();
}
