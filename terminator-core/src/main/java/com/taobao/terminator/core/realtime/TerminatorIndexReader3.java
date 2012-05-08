/*package com.taobao.terminator.core.realtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.FilterIndexReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;

import com.taobao.terminator.common.timers.Job;
import com.taobao.terminator.common.timers.TerminatorSchedulerFactory;

*//**
 * 实时请求中有的请求需要删除磁盘索引上的Document，方便起见，这种删除我们直接作用于对应索引的IndexReader,默认情况下
 * IndexReader的deleteDoc方法是立马生效的，但是有些情况下我们不希望那个这种删除是立马生效的，希望删除能在内存中缓冲
 * 一会儿，除非外部调用flushDeletions()方法，否则删除不起作用
 * 
 * @author yusen
 *//*
public class TerminatorIndexReader extends FilterIndexReader{
	protected static Log log = LogFactory.getLog(TerminatorIndexReader.class);
	private List<Integer> deletions;
	private FlushPolicy policy = new DefaultFlushPolicy(this,100000);
	private DocIdMapper mapper = null;
	public static final String TERM_VALUE = "_UID_";
	public static final Term UID_TERM = new Term("_ID",TERM_VALUE);
	
	public TerminatorIndexReader(IndexReader in,FlushPolicy policy) {
		super(in);
		
		deletions = new ArrayList<Integer>();
		
		if(policy != null) {
			this.policy = policy;
			this.policy.initListener();
		}
		
		try {
			long startTime = System.currentTimeMillis();
			this.mapDocIdAndUid();
			log.warn("UID映射DocId花费的时间是:{" + ((System.currentTimeMillis() - startTime) / 1000) + " s}");
		} catch (IOException e) {
			mapper = null;
			log.warn("UDI映射DocId时出现异常，故接下来不使用映射，直接termDoc命中.",e);
		}
	}
	
	protected void mapDocIdAndUid() throws IOException {
		int maxDoc = this.maxDoc();
		long[] uids = new long[maxDoc];
		TermPositions tp = null;
		byte[] payloadBuffer = new byte[8];
		try {
			tp = this.termPositions(UID_TERM);
			int idx = 0;
			while(tp.next()) { //next()方法会跳过已经删除的
				int doc = tp.doc();
				
				while(idx<doc) {
					uids[idx++] = Integer.MIN_VALUE;
				}
				
				tp.nextPosition();
				tp.getPayload(payloadBuffer, 0);
				
				long uid = bytesToLong(payloadBuffer);
				uids[idx++] = uid;
			}
			
			while(idx < maxDoc) {
				uids[idx++] = Integer.MIN_VALUE;
			}
		} finally {
			if(tp != null) {
				tp.close();
			}
		}
		
		mapper = new DocIdMapper(uids); 
	}
	
	*//**
	 * 
	 * @param idTerm
	 * @return
	 * @throws IOException
	 *//*
	public int termDocFromMapper(Term idTerm) throws IOException {
		if(mapper != null) {
			return mapper.getDocID(Long.valueOf(idTerm.text()));
		} else  {
			TermDocs td = this.termDocs(idTerm);
			if(td.next()) {
				return td.doc();
			}
			return Integer.MIN_VALUE;
		}
	}
	
	private static long bytesToLong(byte[] bytes){
        return 
        	   ((long)(bytes[7] & 0xFF) << 56) 
        	 | ((long)(bytes[6] & 0xFF) << 48)
        	 | ((long)(bytes[5] & 0xFF) << 40) 
        	 | ((long)(bytes[4] & 0xFF) << 32) 
        	 | ((long)(bytes[3] & 0xFF) << 24) 
        	 | ((long)(bytes[2] & 0xFF) << 16)
        	 | ((long)(bytes[1] & 0xFF) <<  8) 
        	 |  (long)(bytes[0] & 0xFF);
	}

	public void markToDelete(int n) {
		synchronized (deletions) {
			deletions.add(n);
		}
	}
	
	*//**
	 * 将缓存在List里的待删除的DocId在IndexReader上生效，即逐一调用IndexReader.deleteDocument方法
	 * @throws IOException
	 *//*
	public void flushDeletions() throws IOException {
		long start = System.currentTimeMillis();
		
		Integer docIds[] = null;
    	synchronized(deletions) {
    		docIds = deletions.toArray(new Integer[0]);
    		deletions.clear();
    	}
    	
    	 * !!!!!!!
    	 * 注意:将删除作用于IndexReader，但是这个时候mapper里边依然存在这个UID --> DocId的映射，当还有相同的已经被删除掉的UID请求删除时
    	 * 仍然能够返回没删除前的DocId，并经DocId再次标记为删除，这个影响不大，不会影响正确性的，大不了重新标记一次，下次flush的时候再删除
    	 * 一次
    	 * 
    	if(docIds != null && docIds.length > 0) {
    		for (Integer docId : docIds) {
    			in.deleteDocument(docId);
    		}
    		//此处的flush会严重影响实时的性能
    		//in.flush();
    	}
    	
    	log.warn("Flush-Deletion-Time==>" + (System.currentTimeMillis() - start));
	}
	
	
	public List<Integer> getDeletions() {
		return deletions;
	}

	public void setDeletions(List<Integer> deletions) {
		this.deletions = deletions;
	}


	*//**
	 * 将内存中的删除ID列表flush到索引中的策略
	 * 
	 * @author yusen
	 *
	 *//*
	public abstract class FlushPolicy {
		protected TerminatorIndexReader indexReader;
		
		public FlushPolicy(){}
		
		public FlushPolicy(TerminatorIndexReader reader){
			this.indexReader = reader;
		}
		
		public void initListener() {
			ScheduledThreadPoolExecutor scheduler = TerminatorSchedulerFactory.newScheduler();
			scheduler.scheduleWithFixedDelay(new Listener(), 1, 1, TimeUnit.SECONDS);
		}
		
		protected abstract boolean needFlushDeletions(List<Integer> deletions,TerminatorIndexReader indexReader);
		
		private class Listener extends Job {

			@Override
			public void doJob() throws Throwable {
				if(needFlushDeletions(deletions, indexReader)){
					indexReader.flushDeletions();
				}
			}
		}
	}
	
	*//**
	 * 默认策略，为了方式内存中保存的deltions过多，造成OOM，此策略会设置一个阀值，当总数量达到一定数量后，会调用TerminatorIndexReader的flushDeletions方法
	 * 
	 * @author yusen
	 *
	 *//*
	public class DefaultFlushPolicy extends FlushPolicy{

		public DefaultFlushPolicy(TerminatorIndexReader reader,int maxNum) {
			this.maxNum =maxNum;
			this.indexReader = reader;
		}
		
		public int maxNum = 1000;
		
		@Override
		protected boolean needFlushDeletions(List<Integer> deletions, TerminatorIndexReader indexReader) {
			return deletions.size() >= maxNum;
		}
	}
}
*/