package com.taobao.terminator.core.realtime;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Deprecated
public class MasterListener {
	public static final String REPORTER_ZK  = "reporter_zk"; //来自ZK的判断
	public static final String REPORTER_CS  = "reporter_cs"; //来自Config-Server的判断
	public static final String REPORTER_SELF = "reporter_self"; //自身Slave同步CommitLog链接断开了
	public static final String REPORTER_TRIGGER = "reporter_trigger"; //通过外部，比如curl的方式主动出发的方式

	public static Map<String,Integer> reporterWeights;

	public static int weightCount = 0;
	private int currentCount = 0;
	private CallBack callback;
	
	static {
		reporterWeights = new HashMap<String,Integer>();
		reporterWeights.put(REPORTER_ZK, 1);
		reporterWeights.put(REPORTER_CS, 1);
		reporterWeights.put(REPORTER_SELF, 1);
		reporterWeights.put(REPORTER_SELF, 3);
		
		Iterator<Integer> itr = reporterWeights.values().iterator();
		while(itr.hasNext()) {
			weightCount = weightCount + itr.next();
		}
	}
	
	private static final MasterListener masterListener = new MasterListener();
	
	public static MasterListener getInstance() {
		return masterListener;
	}
	
	private MasterListener() {
		this.currentCount = weightCount;
	}
	
	public synchronized boolean report(String reporterName) {
		if(reporterWeights.containsKey(reporterName)) {
			currentCount = currentCount - reporterWeights.get(reporterName);
		}
		
		if(currentCount == 0) {
			this.callback.call();
			return true;
		}
		
		return false;
	}
	
	public synchronized void reset() {
		this.currentCount = weightCount;
	}
	
	public CallBack registerCallBack(CallBack callback) {
		final CallBack oldCallback = this.callback;
		this.callback = callback;
		return oldCallback;
	}
	
	public interface CallBack {
		public void call();
	}
}
