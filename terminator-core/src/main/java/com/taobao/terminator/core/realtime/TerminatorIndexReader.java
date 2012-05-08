package com.taobao.terminator.core.realtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FilterIndexReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermPositions;

import com.taobao.terminator.common.timers.Job;
import com.taobao.terminator.common.timers.TerminatorSchedulerFactory;
import com.taobao.terminator.core.fieldx.RangeField;
import com.taobao.terminator.core.realtime.DefaultSearchService.Range;
import com.taobao.terminator.core.realtime.DefaultSearchService.RangeThreadLocalContext;

/**
 * 实时请求中有的请求需要删除磁盘索引上的Document，方便起见，这种删除我们直接作用于对应索引的IndexReader,默认情况下
 * IndexReader的deleteDoc方法是立马生效的，但是有些情况下我们不希望那个这种删除是立马生效的，希望删除能在内存中缓冲
 * 一会儿，除非外部调用flushDeletions()方法，否则删除不起作用
 * 
 * @author yusen
 */
public class TerminatorIndexReader extends FilterIndexReader{
	protected static Log log = LogFactory.getLog(TerminatorIndexReader.class);
	private List<Integer> deletions;
	private DocIdMapper mapper = null;
	public static final String TERM_VALUE = "_UID_";
	public static final Term UID_TERM = new Term("_ID",TERM_VALUE);
	private boolean isFSDir = true;
	
	

	volatile private boolean forceDecRef = false;

	/**
	 * 为了避免老的Core的索引文件的文件句柄没有释放的问题，当FullDumpJob全量完毕之后调用Old-SolrCore的对应的所有的IndexReader
	 * ，将forceDecRef设置为true,系统将会绕开计数器，直接将对应的IndexReader进行close操作
	 */
	public synchronized void decRef() throws IOException {
		if (forceDecRef) {
			try {
				ensureOpen();
				commit();
				doClose();
			} finally {
				forceDecRef = false;
			}
		} else {
			super.decRef();
		}
		log.warn("test decRef end");
	}
	
	public void setForceDecRef() {
		this.forceDecRef = true;
	}
	
	public TerminatorIndexReader(IndexReader in, boolean isFSDir) {
		super(in);
		/* 
		 * 只有磁盘索引需要UID-->DocID的映射和Range字段的映射。
		 * 内存索引的时候，由于是直接读内存的，性能已经非常不错了。也就没有太大的必要在内存索引之上再搞一次缓存了
		 * */
		
		deletions = new ArrayList<Integer>();
		if (this.isFSDir = isFSDir) { //磁盘索引才做映射，内存索引没有必要做映射了
			try {
				long startTime = System.currentTimeMillis();
				this.mapDocIdAndUid();
				log.warn("UID映射DocId花费的时间是:{" + ((System.currentTimeMillis() - startTime) / 1000) + " s}");
			} catch (IOException e) {
				mapper = null;
				log.warn("UDI映射DocId时出现异常，故接下来不使用映射，直接termDoc命中.", e);
			}
			
			//读取需要区间查询的字段的所有的值，放在内存中
			try {
				long startTime = System.currentTimeMillis();
				this.readRangeFields();
				log.warn("读取RangeField的时间是:{" + ((System.currentTimeMillis() - startTime) / 1000) + " s}");
			} catch (IOException e) {
				log.warn("读取RangeField时出现异常", e);
			}
		}
	}
	
	/**
	 * ！！！
	 * 此处的方法重载很重要！
	 * 由于Solr回将所有的IndexReader的subReader拿出来，做了一次扁平化的处理，处理之后，termDoc方法将直接调用subReader的termDoc方法
	 * 为了使下面的termDoc(Term term)方法能被调到，研究Solr代码之后做了这样丑陋的方法重载！
	 */
	@Override
	public IndexReader[] getSequentialSubReaders() {
		return null;
	}

	protected Map<String,RangeFileds> rangeNums = new ConcurrentHashMap<String,RangeFileds>();
	
	@SuppressWarnings("unchecked")
	protected void readRangeFields() throws IOException {
		/* 获取需要区间查询的字段，需要区间查询的字段以RANGE_FIELD_开头 */
		Collection fns = super.in.getFieldNames(FieldOption.ALL);
		Iterator itr = fns.iterator();
		Set<String> rfs = new HashSet<String>();
		while (itr.hasNext()) {
			String fn = (String) itr.next();
			if (fn.startsWith("RF_")) {
				rfs.add(fn);
			}
		}

		Iterator<String> itr2 = rfs.iterator();
		while (itr2.hasNext()) {
			String fn = itr2.next();
			char typeC = fn.charAt(3);
			if(typeC == 'L') {
				this.readLongRangeField(fn, fn);
			} else if(typeC == 'I') {
				this.readIntRangeField(fn, fn);
			} else if(typeC == 'S') {
				this.readShortRangeField(fn, fn);
			}
		}
	}
	
	private void readIntRangeField(String fieldName,String termText) throws IOException{
		int maxDoc = this.maxDoc();
		RangeFileds rf  = new RangeFileds(RangeField.TYPE_INT, maxDoc);
		TermPositions tp = null;
		byte[] payloadBuffer = new byte[4];
		
		try {
			tp = this.termPositions(new Term(fieldName, termText));
			int idx = 0;
			while (tp.next()) {
				int doc = tp.doc();

				while (idx < doc) {
					rf.set(idx++, Integer.MIN_VALUE);
				}

				tp.nextPosition();
				tp.getPayload(payloadBuffer, 0);

				int num = Utils.bytesToInt(payloadBuffer);
				rf.set(idx++, num);
			}

			while (idx < maxDoc) {
				rf.set(idx++, Integer.MIN_VALUE);
			}
			this.rangeNums.put(fieldName, rf);
		} finally {
			if (tp != null) {
				tp.close();
			}
		}
	}
	
	private void readShortRangeField(String fieldName,String termText)throws IOException {
		int maxDoc = this.maxDoc();
		RangeFileds rf  = new RangeFileds(RangeField.TYPE_SHORT, maxDoc);
		TermPositions tp = null;
		byte[] payloadBuffer = new byte[2];
		
		try {
			tp = this.termPositions(new Term(fieldName, termText));
			int idx = 0;
			while (tp.next()) {
				int doc = tp.doc();

				while (idx < doc) {
					rf.set(idx++, Short.MIN_VALUE);
				}

				tp.nextPosition();
				tp.getPayload(payloadBuffer, 0);

				short num = Utils.bytesToShort(payloadBuffer);
				rf.set(idx++, num);
			}

			while (idx < maxDoc) {
				rf.set(idx++, Short.MIN_VALUE);
			}
			this.rangeNums.put(fieldName, rf);
		} finally {
			if (tp != null) {
				tp.close();
			}
		}
	}
	
	private void readLongRangeField(String fieldName,String termText) throws IOException {
		int maxDoc = this.maxDoc();
		RangeFileds rf  = new RangeFileds(RangeField.TYPE_LONG, maxDoc);
		TermPositions tp = null;
		byte[] payloadBuffer = new byte[8];
		
		try {
			tp = this.termPositions(new Term(fieldName, termText));
			int idx = 0;
			while (tp.next()) {
				int doc = tp.doc();

				while (idx < doc) {
					rf.set(idx++, Long.MIN_VALUE);
				}

				tp.nextPosition();
				tp.getPayload(payloadBuffer, 0);

				long num = Utils.bytesToLong(payloadBuffer);
				rf.set(idx++, num);
			}

			while (idx < maxDoc) {
				rf.set(idx++, Long.MIN_VALUE);
			}
			this.rangeNums.put(fieldName, rf);
		} finally {
			if (tp != null) {
				tp.close();
			}
		}
	}
	
	public synchronized TerminatorIndexReader reopen() throws CorruptIndexException, IOException {
		IndexReader newReader = in.reopen();
		/*if (newReader != in) {
			in.close();
		}*/
		return new TerminatorIndexReader(newReader, true);
	}
	
	public synchronized TerminatorIndexReader reopenWithoutMapper() throws CorruptIndexException, IOException {
		IndexReader newReader = in.reopen();
		/*if (newReader != in) {
			in.close();
		}*/
		return new TerminatorIndexReader(newReader, false);
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
				
				long uid = Utils.bytesToLong(payloadBuffer);
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
	
	/**
	 * 读取对应的UID的Term对应的DocId<br>
	 * 如果是磁盘索引，则从缓存映射中拿出来<br>
	 * 如果是内存索引，则直接termDocs查询一下即可
	 * @param idTerm
	 * @return
	 * @throws IOException
	 */
	public int termDocFromMapper(Term idTerm) throws IOException {
		if(mapper != null) {
			return mapper.getDocID(Long.valueOf(idTerm.text()));
		} else  {
			TermDocs td = super.termDocs(idTerm);
			if(td.next()) {
				return td.doc();
			}
			return Integer.MIN_VALUE;
		}
	}

	/**
	 * 标记删除，并不直接作用于IndexReader，只是标记一下 ，如果需要作用于IndexReader或者直接作用于索引文件的话，调用flushDeletions(boolean persistenDel)
	 * @param n
	 */
	public void markToDelete(int n) {
		synchronized (deletions) {
			deletions.add(n);
		}
	}
	
	/**
	 * 将缓存在List里的待删除的DocId在IndexReader上生效，即逐一调用IndexReader.deleteDocument方法
	 * 
	 * @param persistenDel 是否将删除持久化作用于索引文件
	 * @throws IOException
	 */
	public void flushDeletions(boolean persistenDel) throws IOException {
		long start = System.currentTimeMillis();
		
		Integer docIds[] = null;
    	synchronized(deletions) {
    		docIds = deletions.toArray(new Integer[0]);
    		deletions.clear();
    	}
    	
    	/* * !!!!!!!
    	 * 注意:将删除作用于IndexReader，但是这个时候mapper里边依然存在这个UID --> DocId的映射，当还有相同的已经被删除掉的UID请求删除时
    	 * 仍然能够返回没删除前的DocId，并经DocId再次标记为删除，这个影响不大，不会影响正确性的，大不了重新标记一次，下次flush的时候再删除
    	 * 一次
    	 * */
    	if(docIds != null && docIds.length > 0) {
    		for (Integer docId : docIds) {
    			in.deleteDocument(docId);
    		}
    		//此处的flush会严重影响实时的性能
    		//in.flush();
    	}
    	
    	if(persistenDel) {
    		in.flush();
    	}
    	
    	log.warn("Flush-Deletion-Time==>" + (System.currentTimeMillis() - start));
	}
	
	
	protected synchronized void acquireWriteLock() throws IOException {
	    /* NOOP */
	}
	
	public List<Integer> getDeletions() {
		return deletions;
	}

	public void setDeletions(List<Integer> deletions) {
		this.deletions = deletions;
	}

	
	/**
	 * 某一次查询为Q1,其中Q1由Qn(去除Qn后的条件)和Qr(区间条件)组成，优化后的区间查询如下:<br>
	 * 现有Qn查询，然后将Qn查询出来的结果于Qr做比较，看对应的结果是否在指定的字段中，不在则抛弃之。<br>
	 * 
	 * 具体的实现参照lucene的相关代码。
	 * 
	 */
	//@Override
	public TermDocs termDocs(Term term) throws IOException {
		final Map<String,Range> ranges = RangeThreadLocalContext.getInstance().get();
		return new FilterTermDocs(super.termDocs(term)) {
			@Override
			public boolean next() throws IOException {
				boolean res;
				while ((res = super.next())) {
					if (isInRange(doc(),ranges)) {
						break;
					}
				}
				return res;
			}
			
			@Override
			public int read(int[] docs, int[] freqs) throws IOException {
				int num = super.read(docs, freqs);
				if(num == 0) {
					return 0;
				} else {
					if(ranges == null) {
						return num;
					}
					int delNum = 0;
					for(int i =0;i<num;i++) {
						int doc = docs[i];
						if(!isInRange(doc,ranges)) {
							delNum  = delNum + 1;
							docs[i] = -1;
							freqs[i] = -1;
						}
					}
					
					if(delNum == 0) { //没有过滤掉
						return num;
					} else if(delNum == num) { //全部过滤掉了
						return read(docs,freqs);
					} else if(delNum > 0) { //过滤掉一部分
						int[] newdocs = new int[num - delNum];
						int[] newfreqs = new int[num - delNum];
						int newi = 0;
						for(int i = 0;i <num;i++) {
							if(docs[i] != -1) {
								newdocs[newi] = docs[i];
								newfreqs[newi] = freqs[i];
								newi++;
							}
						}
						
						int i = 0;
						for(i = 0;i<newdocs.length;i++) {
							docs[i] = newdocs[i];
							freqs[i] = newfreqs[i];
						}
						
						for(int j = i;j<docs.length;j++) {
							docs[j] = Integer.MIN_VALUE;
							freqs[j] = 0;
						}
						
						return newdocs.length;
					} else {
						return num;
					}
				}
			}
		};
	}
	
	public TermDocs termDocs() throws IOException {
		final Map<String,Range> ranges = RangeThreadLocalContext.getInstance().get();
		return new FilterTermDocs(super.termDocs()) {
			@Override
			public boolean next() throws IOException {
				boolean res;
				while ((res = super.next())) {
					if (isInRange(doc(),ranges)) {
						break;
					}
				}
				return res;
			}
			
			@Override
			public int read(int[] docs, int[] freqs) throws IOException {
				int num = super.read(docs, freqs);
				if(num == 0) {
					return 0;
				} else {
					if(ranges == null) {
						return num;
					}
					int delNum = 0;
					for(int i =0;i<num;i++) {
						int doc = docs[i];
						if(!isInRange(doc,ranges)) {
							delNum  = delNum + 1;
							docs[i] = -1;
							freqs[i] = -1;
						}
					}
					
					if(delNum == 0) { //没有过滤掉
						return num;
					} else if(delNum == num) { //全部过滤掉了
						return read(docs,freqs);
					} else if(delNum > 0) { //过滤掉一部分
						int[] newdocs = new int[num - delNum];
						int[] newfreqs = new int[num - delNum];
						int newi = 0;
						for(int i = 0;i <num;i++) {
							if(docs[i] != -1) {
								newdocs[newi] = docs[i];
								newfreqs[newi] = freqs[i];
								newi++;
							}
						}
						
						
						for(int i = 0;i<newdocs.length;i++) {
							docs[i] = newdocs[i];
							freqs[i] = newfreqs[i];
						}
						
						return newdocs.length;
					} else {
						return num;
					}
				}
			}
		};
	}
	
	@SuppressWarnings("unchecked")
	protected boolean isInRange(int docId,Map<String,Range> ranges) throws IOException{
		if(docId < 0) {
			return false;
		}
		if(ranges == null) {
			return true;
		}
		
		//格式为RF_S_count
		final Set<String> rangeFieldNames = ranges.keySet();
		
		if(isFSDir) {
			for(String fn : rangeFieldNames) {
				RangeFileds rf = this.rangeNums.get(fn);
				
				long num = 0L;
				if (rf.type == RangeField.TYPE_INT) {
					num = rf.getInt(docId);
				} else if (rf.type == RangeField.TYPE_LONG) {
					num = rf.getLong(docId);
				} else if (rf.type == RangeField.TYPE_SHORT) {
					num = rf.getShort(docId);
				}
				
				Range range = ranges.get(fn);
				if(!range.inRange(num)) {
					return false;
				}
			}
		} else { 
			/*
			  *内存里的数据，就没有必要从数组里边拿了，直接用IndexReader命中document就好了，反正是从内存里出来的，已经很快了，
			  * 然后拿到相应的区间字段，比较一下是否在指定的区间里边。
			  * 这样做的目的还是为了尽量的降低内存的消耗！
		      **/
			//上层传过来的FieldName是处理过后的，类似于RF_S_count RF_I_am的形式
			Map<String,Range> tmpRangeMaps = new HashMap<String,Range>();
			for(String fn : rangeFieldNames) {
				String realname = fn.substring(5);
				tmpRangeMaps.put(realname, ranges.get(fn));
			}
			
			final Set<String> s = tmpRangeMaps.keySet();
			final Document doc = this.document(docId,new FieldSelector(){
				private static final long serialVersionUID = 5181398833132100319L;

				@Override
				public FieldSelectorResult accept(String fieldName) {
					if(s.contains(fieldName)) {
						return FieldSelectorResult.LOAD;
					} else  {
						return FieldSelectorResult.NO_LOAD;
					}
				}});
			
			final List fl = doc.getFields();
			for(Object o : fl) {
				Fieldable f = (Fieldable)o;
				String name = f.name();
				Range r = tmpRangeMaps.get(name);
				String external = r.ft.toExternal(f);
				if(!r.inRange(Long.valueOf(external))) {
					return false;
				}
			}
		}

		return true;
	}
	
	protected class RangeFileds {
		int type;
		int[] inums;
		short[] snums;
		long[] lnums;

		public RangeFileds (int type,int size) {
			this.type = type;
			if (type == RangeField.TYPE_INT) {
				inums = new int[size];
			} else if (type == RangeField.TYPE_LONG) {
				lnums = new long[size];
			} else if (type == RangeField.TYPE_SHORT) {
				snums = new short[size];
			} else {
				throw new IllegalArgumentException("Type ERROR");
			}
		}

		public int getInt(int docId) {
			if(type == RangeField.TYPE_INT) {
				return inums[docId];
			} else {
				throw new IllegalStateException("Current Type is not INT!");
			}
		}
		
		public long getLong(int docId) {
			if(type == RangeField.TYPE_LONG) {
				return lnums[docId];
			}else {
				throw new IllegalStateException("Current Type is not LONG!");
			}
		}
		
		public short getShort(int docId) {
			if(type == RangeField.TYPE_SHORT) {
				return snums[docId];
			} else {
				throw new IllegalStateException("Current Type is not SHORT!");
			}
		}

		public void set(int i, short num) {
			if(type == RangeField.TYPE_SHORT) {
				snums[i] = num;
			} else {
				throw new IllegalStateException("Current Type is not SHORT!");
			}
		}

		public void set(int i, int num) {
			if(type == RangeField.TYPE_INT) {
				inums[i] = num;
			} else {
				throw new IllegalStateException("Current Type is not INT!");
			}
		}

		public void set(int i, long num) {
			if(type == RangeField.TYPE_LONG) {
				lnums[i] = num;
			} else {
				throw new IllegalStateException("Current Type is not LONG!");
			}
		}
	}
	
	public DocIdMapper getMapper() {
		return mapper;
	}

	public void setMapper(DocIdMapper mapper) {
		this.mapper = mapper;
	}

	public boolean isFSDir() {
		return isFSDir;
	}

	public void setFSDir(boolean isFSDir) {
		this.isFSDir = isFSDir;
	}

	/**
	 * byte编码工具类
	 * 
	 * @author yusen
	 */
	public static class Utils {
		public static byte[] toBytes(int i) throws IOException {
			ByteArrayOutputStream bout = new ByteArrayOutputStream(1);
			while ((i & ~0x7F) != 0) {
				bout.write(((byte) ((i & 0x7f) | 0x80)));
				i >>>= 7;
			}
			bout.write(((byte) i));
			return bout.toByteArray();
		}
		
		public static int toInt(byte[] bs) throws IOException{
			ByteArrayInputStream bin = new ByteArrayInputStream(bs);
			byte b = (byte)bin.read();
			int i = b & 0x7F;
			for (int shift = 7; (b & 0x80) != 0; shift += 7) {
				b = (byte)bin.read();
				i |= (b & 0x7F) << shift;
			}
			return i;
		}

		public static long bytesToLong(byte[] bytes) {
			return ((long) (bytes[7] & 0xFF) << 56) | ((long) (bytes[6] & 0xFF) << 48) | ((long) (bytes[5] & 0xFF) << 40)
					| ((long) (bytes[4] & 0xFF) << 32) | ((long) (bytes[3] & 0xFF) << 24) | ((long) (bytes[2] & 0xFF) << 16)
					| ((long) (bytes[1] & 0xFF) << 8) | (long) (bytes[0] & 0xFF);
		}

		public static int bytesToInt(byte[] bytes) {
			return ((bytes[3] & 0xFF) << 24) | ((bytes[2] & 0xFF) << 16) | ((bytes[1] & 0xFF) << 8) | (bytes[0] & 0xFF);
		}
		
		public static short bytesToShort(byte[] bytes) {
			return (short) (((bytes[1] & 0xFF) << 8) | (bytes[0] & 0xFF));
		}
	}
	
	public static void main(String[] args){
		try {
			short a = Short.MAX_VALUE;
			System.out.println(Utils.toInt(Utils.toBytes(12455)));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 将内存中的删除ID列表flush到索引中的策略
	 * 
	 * @author yusen
	 * @deprecated
	 */
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
					indexReader.flushDeletions(true);
				}
			}
		}
	}
	
	/**
	 * 默认策略，为了方式内存中保存的deltions过多，造成OOM，此策略会设置一个阀值，当总数量达到一定数量后，会调用TerminatorIndexReader的flushDeletions方法
	 * 
	 * @author yusen
	 * @deprecated
	 */
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
