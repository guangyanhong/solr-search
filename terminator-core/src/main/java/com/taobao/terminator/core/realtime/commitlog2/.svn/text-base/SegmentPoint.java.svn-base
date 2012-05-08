package com.taobao.terminator.core.realtime.commitlog2;

import java.io.Serializable;
import java.util.Date;

/**
 * 记录CommitLog的的片段文件(segment)的位置
 * @author yusen
 *
 */
public class SegmentPoint implements Serializable{
	private static final long serialVersionUID = 1L;
	
	private long time = -1L;
	private String segmentName;
	private long offset;
	
	private long segmenNum = -1L;
	
	public SegmentPoint(long time,String segmentName, long offset) {
		if(!CommitLogUtils.isSegment(segmentName)) {
			throw new IllegalArgumentException("SegmentName ==> " + segmentName);
		}
		if(time <= -1L) {
			time = System.currentTimeMillis();
		}
		
		this.time = time;
		this.segmentName = segmentName;
		this.offset = offset;
	}
	
	public SegmentPoint(long time,long segmentNum,long offset) {
		this.time = time;
		this.segmenNum = segmentNum;
		this.segmentName = CommitLogUtils.genSegmentName(segmentNum);
		this.offset = offset;
		if(this.time == -1L) {
			this.time = System.currentTimeMillis();
		}
	}

	public String getSegmentName() {
		return segmentName;
	}

	public void setSegmentName(String segmentName) {
		this.segmentName = segmentName;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}
	
	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public long getSegmentNum() {
		if(segmenNum == -1) {
			segmenNum = CommitLogUtils.getSegmentNum(segmentName);
		}
		return segmenNum;
	}

	@Override
	public String toString() {
		return "SegmentPoint [offset=" + offset + ", segmenNum=" + segmenNum + ", segmentName=" + segmentName + ", time=" +  new Date(time) + "]";
	}
}
