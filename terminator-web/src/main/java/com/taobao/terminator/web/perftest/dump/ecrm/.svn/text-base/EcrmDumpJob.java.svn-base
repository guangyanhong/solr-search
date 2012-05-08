package com.taobao.terminator.web.perftest.dump.ecrm;

import java.io.File;
import java.util.Date;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class EcrmDumpJob implements Callable<Integer> {
	protected Log logger = LogFactory.getLog(EcrmDumpJob.class);

	public static final int ERROR_OTHERS = -1;
	public static final int ERROR_FILE_NOT_EXIST = -2;
	public static final int ERROR_SHELL_RUNTIME = -3;
	public static final int NORM_CODE = 0;

	private String shellPath = null;
	private String groupNum = null;

	public EcrmDumpJob(String shellPath, String groupNum) {
		this.shellPath = shellPath;
		this.groupNum = groupNum;
	}
	
	public static void main(String[] args) {
		long current = System.currentTimeMillis();
		long d = current / (1000 * 60 * 60 * 24);
		System.out.println(new Date(1970+d * 1000 * 60 * 60 * 24));
	}

	/**
	 * @return 0£ºÕý³£·µ»Ø
	 */
	public Integer call() throws Exception {
		int resultCode = ERROR_OTHERS;
		File file = new File(this.shellPath);
		if (file.exists() && file.isFile()) {
			Runtime run = Runtime.getRuntime();
			Process process = run.exec(this.shellPath + " " + groupNum);
			resultCode = process.waitFor();
		} else {
			return ERROR_FILE_NOT_EXIST;
		}
		return resultCode;
	}
}
