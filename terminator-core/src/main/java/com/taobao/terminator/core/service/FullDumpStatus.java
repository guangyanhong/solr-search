package com.taobao.terminator.core.service;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FullDumpStatus {
	public static final int stage_waiting     = 0;
	public static final int stage_started     = 1;
	public static final int stage_transmiting = 2;
	public static final int stage_finishing   = 3;
	public static final int stage_finished    = 4;
	public static final int stage_swapingcore = 5;
	
	public AtomicBoolean isFullIndexJobRunning = new AtomicBoolean(false);
	
	public AtomicInteger dumpCount = new AtomicInteger(0);
	
	private int stage = 0;
	
	public boolean canStart(){
		return stage == stage_started || stage == stage_waiting;
	}
	
	public boolean canTransmit(){
		return stage == stage_transmiting || stage == stage_started;
	}
	
	public boolean canSwap(){
		return stage == stage_finished;
	}

	public void setStage(int stage){
		this.stage = stage;
	}
	
	public int nextStage(){
		return stage ++;
	}
	
	public int getCurrenStage(){
		return stage;
	}
}
