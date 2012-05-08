package com.taobao.terminator.client.index.buffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 数据本地缓冲区
 * 
 * @author yusen
 */
public class DataBuffer implements Buffer{
	protected static Log log = LogFactory.getLog(DataBuffer.class);
	
	protected int capacityIncrement = CapacityInfo.DEFAULT_CAPACITYINCREMENT;
	protected int initcapacity = CapacityInfo.DEFAULT_INITCAPACITY;
	protected byte[] buffer;
	protected int bufferUsage = 0;
	protected int maxCapacity = CapacityInfo.DEFAULT_MAXCAPACITY;
	
	public DataBuffer(){
		this(0,0,0);
	}
	
	public DataBuffer(int initcapacity){
		this(initcapacity,0,0);
	}
	
	public DataBuffer(int initcapacity,int maxCapacity,int capacityIncrement){
		this.initcapacity = (initcapacity <= 0 ?  CapacityInfo.DEFAULT_INITCAPACITY : initcapacity);
		this.maxCapacity = (maxCapacity <= 0 ? CapacityInfo.DEFAULT_INITCAPACITY * 2 :maxCapacity);
		this.capacityIncrement = capacityIncrement;
		this.buffer = new byte[this.initcapacity];
		this.bufferUsage = 0;
	}
	
	public DataBuffer(CapacityInfo info){
		this(info.initcapacity, info.maxCapacity, info.capacityIncrement);
	}
	
	public synchronized void append(byte[] data) {
		this.ensureRelocate(data);
		System.arraycopy(data, 0, this.buffer, this.bufferUsage, data.length);
		this.bufferUsage += data.length;
	}
	
	public synchronized boolean isOverFlow(){
		return this.bufferUsage > this.maxCapacity;
	}
	
	public synchronized void reset(){
		this.buffer = new byte[initcapacity];
		this.bufferUsage = 0;
	}
	
	private void ensureRelocate(byte[] data){
		int count = data.length + this.bufferUsage;
		if(count > this.buffer.length){
			int oldCapacity = this.buffer.length;
			int newCapacity = (capacityIncrement > 0) ? (oldCapacity + capacityIncrement) : (oldCapacity * 2);
			if(newCapacity < count){
				newCapacity = count;
			}
			byte[] newBuffer = new byte[newCapacity];
			System.arraycopy(this.buffer, 0, newBuffer, 0, this.bufferUsage);
			this.buffer = newBuffer;
		}
	}
	
	public int getCapacityIncrement() {
		return capacityIncrement;
	}

	public void setCapacityIncrement(int capacityIncrement) {
		this.capacityIncrement = capacityIncrement;
	}

	public int getInitcapacity() {
		return initcapacity;
	}

	public byte[] getBuffer() {
		return buffer;
	}

	public int getBufferUsage() {
		return bufferUsage;
	}

	public int getMaxCapacity() {
		return maxCapacity;
	}


	/**
	 * DataBuffer的容量信息
	 * 
	 * @author yusen
	 *
	 */
	public static class CapacityInfo{
		public static final int DEFAULT_INITCAPACITY      = 1024 * 1024 * 2;
		public static final int DEFAULT_MAXCAPACITY       = DEFAULT_INITCAPACITY * 2;
		public static final int DEFAULT_CAPACITYINCREMENT = 1024 * 512;
		
		public int initcapacity = DEFAULT_INITCAPACITY;          //初始化容量
		public int maxCapacity  = DEFAULT_MAXCAPACITY;           //最大容量
		public int capacityIncrement = DEFAULT_CAPACITYINCREMENT; //容量的增长量
		public int getInitcapacity() {
			return initcapacity;
		}
		public void setInitcapacity(int initcapacity) {
			this.initcapacity = initcapacity;
		}
		public int getMaxCapacity() {
			return maxCapacity;
		}
		public void setMaxCapacity(int maxCapacity) {
			this.maxCapacity = maxCapacity;
		}
		public int getCapacityIncrement() {
			return capacityIncrement;
		}
		public void setCapacityIncrement(int capacityIncrement) {
			this.capacityIncrement = capacityIncrement;
		}
	}
}
