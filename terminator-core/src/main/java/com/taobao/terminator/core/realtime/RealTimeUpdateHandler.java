package com.taobao.terminator.core.realtime;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.UpdateHandler;

import com.taobao.terminator.core.realtime.commitlog2.CommitLogAccessor;
import com.taobao.terminator.core.realtime.common.Utils;

/**
 * ʵʱ����Ĵ�������ͨ��ֻ��һ���߳�˳��ʵʱ����������������,�ʴ�ʵ�ֲ�δ���Ƕ��̵߳������<b>δ���κ�ͬ������</b>
 * 
 * @author yusen
 */
public class RealTimeUpdateHandler extends UpdateHandler {
	protected static Log log = LogFactory.getLog(RealTimeUpdateHandler.class);
	
	protected IndexWriter ramWriter;
	private String dataDir;
	
	private CommitLogAccessor commitLogAccessor;
	
	/** Ӱ���ڴ�������ͳ�Ʋ�����������ʱ���� */
	private AtomicInteger effectCount = new AtomicInteger(0);
	private long lastCommitTime = System.currentTimeMillis();
	
	
	/** �ύ�ķ�ֵ���Ʋ���  */
	private int commitCountThreshold = 100;
	private int commitTimeThreshold  = 1 * 1000 * 1;
	
	
	/** �ڴ�����д����̵ķ�ֵ���Ʋ���  */
	private int flushCountThreshold = 1500000; //100w�θ���
	private int flushSizeThreshold  = 500 * 1024 * 1024; //500M

	public RealTimeUpdateHandler(SolrCore core) throws IOException {
		super(core);
		if(idField == null) {
			throw new RuntimeException("UniqueKeyFieldName doesn't specify!!!");
		}
		this.dataDir = core.getCoreDescriptor().getDataDir();
		this.initArgs();
		this.initWriter();
	}
	
	private void initArgs() {
		String commitCountThresholdStr =  core.getSolrConfig().get("realTimeArgs/commitPolicy/countThreshold");
		String commitTimeThresholdStr = core.getSolrConfig().get("realTimeArgs/commitPolicy/timeThreshold");
		
		String flushCountThresholdStr = core.getSolrConfig().get("realTimeArgs/flushPolicy/countThreshold");
		String flushSizeThresholdStr = core.getSolrConfig().get("realTimeArgs/flushPolicy/sizeThreshold");
	
		try {
			commitCountThreshold = Integer.valueOf(commitCountThresholdStr);
			commitTimeThreshold = Integer.valueOf(commitTimeThresholdStr);
			
			flushCountThreshold = Integer.valueOf(flushCountThresholdStr);
			flushSizeThreshold = Integer.valueOf(flushSizeThresholdStr);
		} catch (NumberFormatException e) {
			throw  new RuntimeException("/realTimeArgs/��صĲ������ô���,��������ֵ��ʽ.",e);
		}
		
		log.warn("��ʼ��Flush��Commit�ķ�ֵ������Ϣ : commitCountThreshold : {" + commitCountThreshold + "} commitTimeThreshold : {" + commitTimeThreshold +"} flushCountThreshold : {" + flushCountThreshold +"} flushSizeThreshold :  {" + flushSizeThreshold + "}");
	}

	private void initWriter() throws IOException{
		if (ramWriter == null) {
			Directory ramDir = new RAMDirectory();
			ramWriter = createRAMIndexWriter(ramDir);
			this.getIndexReaderFactory().setRamReader(new TerminatorIndexReader(IndexReader.open(ramDir, false),false));
		}
	}

	/**
	 * �����µ��ڴ�������IndexWriter,CommitLog�Ķ��̵߳��ô˷���
	 * 
	 * @param newRAMDirectory
	 * @return
	 * @throws IOException
	 */
	protected IndexWriter createRAMIndexWriter(Directory newRAMDirectory) throws IOException {
		return new IndexWriter(newRAMDirectory, schema.getAnalyzer(), true, core.getDeletionPolicy(), new MaxFieldLength(IndexWriter.DEFAULT_MAX_FIELD_LENGTH));
	}
	
	/**
	 * �����µ��ڴ�����֮ǰҪ��������
	 * 
	 * @param newRAMDirectory
	 * @throws IOException
	 */
	protected void beforCreateNewRamIndexWriter() throws IOException {
		commitLogAccessor.writeFlushAt();
	}

	/**
	 * Add��Update�����ô˽ӿڣ���allowDups����
	 * 
	 * @param cmd
	 * @return
	 * @throws IOException
	 */
	public int addDoc(AddUpdateCommand cmd) throws IOException {
		if (!cmd.allowDups) {
			if (cmd.indexedId == null) {
				cmd.indexedId = getIndexedId(cmd.doc);
			}

			Term idTerm = this.idTerm.createTerm(cmd.indexedId);
			this.updateDocument(idTerm, cmd.getLuceneDocument(schema));
		} else {
			this.getRamWriter().addDocument(cmd.getLuceneDocument(schema));
		}

		effectCount.incrementAndGet();
		this.maybeCommit();
		this.maybeFlush();

		return 1;
	}
	
	/**
	 * ���µ�Doc������ڴ������У���ֱ�Ӳ����ڴ�����
	 * ����Ļ�����ɾ���Ķ��������ڴ���������Ȼ�������Ķ����������ڴ�����
	 * ��֤��������������Doc��add���������ֻ����IndexReader��deleteDocument����
	 * 
	 * @param idTerm
	 * @param luceneDoc
	 * @return
	 * @throws IOException
	 */
	private void updateDocument(Term idTerm, Document luceneDoc) throws IOException {
		this.markToDelete(idTerm); // ��������ɾ������ǣ����Ǽ�����Ч��
		this.getRamWriter().updateDocument(idTerm, luceneDoc);
	}
	
	/* ���ɾ��,��Ҫɾ����DocId������TerminatorIndexReader��List�� */
	private int markToDelete(Term idTerm) throws IOException{
		int stage = 0;
		try {
			int delNum = 0;
			List<TerminatorIndexReader> diskReaders = this.getDiskReaders();
			
			if (diskReaders != null && !diskReaders.isEmpty()) {
				for (TerminatorIndexReader reader : diskReaders) {
					int docId = reader.termDocFromMapper(idTerm);
					if (docId > 0) {
						reader.markToDelete(docId);
						delNum ++;
					}
				}
			}
			stage = 1;
			
			TerminatorIndexReader mainReader = this.getMainReader();
			int docId = mainReader.termDocFromMapper(idTerm);
			
			if (docId >= 0) {
				mainReader.markToDelete(docId);
				delNum ++;
			}
			stage = 2;
			return delNum;
		} catch (IOException e) {
			throw new IOException("[Mark-Delete-From-Disk-Error] {" + (stage == 0 ? "disks-index":"main-index") + "}",e);
		}
	}
	
	public void delete(DeleteUpdateCommand cmd) throws IOException{
		Term term = idTerm.createTerm(idFieldType.toInternal(cmd.id));
		boolean hasDeleted = false;
		
		if (this.getRamReader().termDocs(term).next()) {
			this.getRamWriter().deleteDocuments(term); /* ͨ��Writerɾ����Reader��reopen���ܱ��ֳ��� */
			hasDeleted = true;
		} else {
			hasDeleted = this.markToDelete(term) > 0;
		}
		
		if(hasDeleted) {
			effectCount.incrementAndGet();
			this.maybeCommit();
			this.maybeFlush();
		} 
	}
	
	private void reset(){
		effectCount.set(0);
		lastCommitTime = System.currentTimeMillis();
	}
	
	volatile private boolean isCommiting = false;
	volatile private boolean isFlushing  = false;
	
	/**
	 * �˷���ֻ����BuildIndexJob�̵߳���
	 * 
	 * @throws IOException
	 */
	public boolean maybeCommit() throws IOException {
		
		if(needCommit() && this.ramWriter != null) {
			try {
				isCommiting = true;
				this.commitAll(false);
				this.reset();
				return true;
			} finally {
				isCommiting = false;
			}
		}
		return false;
	}
	
	private static final MergeScheduler seriMergeScheduler =  new SerialMergeScheduler();
	
	
	
	/**
	 * �˷���ֻ����BuildIndexJob�̵߳���
	 * 
	 * @throws IOException
	 */
	public boolean maybeFlush() throws IOException{
		if(needFlush()) {
			try {
				isFlushing = true;
				long start = System.currentTimeMillis();
				//expert:IndexWriterĬ�ϵ�����ConcurrentMergeScheduler������һ���߳̽��кϲ�����֪��ʲôʱ��������첽�ġ�������Ҫ�滻���õ�ǰ�߳�ͬ��ִ��
				this.ramWriter.setMergeScheduler(seriMergeScheduler);
				this.commitAll(true);
				this.flushRamToDisk();
				this.reset();
				log.warn("[Flush-Index-File-To-Disk]�ڴ�����Flush�����̺��ܺ�ʱ:{" + (System.currentTimeMillis() - start) + "-ms}");
				return true;
			} finally {
				isFlushing = false;
			}
		}
		return false;
	}
	
	/**
	 * �ж��Ƿ���ҪCommit����
	 * 
	 * @return
	 */
	protected boolean needCommit() {
		return  effectCount.get() >= this.commitCountThreshold  || 
				(System.currentTimeMillis() - lastCommitTime) >= this.commitTimeThreshold;
	}
	
	protected boolean needFlush() {
		int maxDoc = this.ramWriter.maxDoc();
		long ramSize = ((RAMDirectory)(this.getRamWriter().getDirectory())).sizeInBytes();
		
		boolean overNum = maxDoc >= this.flushCountThreshold;
		boolean overSize = ramSize >= this.flushSizeThreshold;
		
		if(overNum || overSize) {
			log.warn("[Flush-Index-File-To-Disk]�ڴ�����������Ʒ�ֵ����Flush��������,�ڴ���������Doc����{" + maxDoc +"},�ڴ������ܴ�С {" + ramSize/(1024*1024) +" M}");
			return true;
		} 

		return false;
	}
	
	@SuppressWarnings("unchecked")
	protected void commitAll(boolean persistenDel)  throws IOException {
		try {
			/* 1.�ڴ������ı仯�ύ��  */
			this.ramWriter.commit(); 
			
			/* 2.��������TerminatorIndexReader��delList������IndexReader��  */
			this.getMainReader().flushDeletions(persistenDel);
			List<TerminatorIndexReader> diskReaders = this.getDiskReaders();
			if(diskReaders != null && !diskReaders.isEmpty()) {
				for (TerminatorIndexReader diskReader : diskReaders) {
					diskReader.flushDeletions(persistenDel);
				}
			}
			
			/* *
			 * PS.���϶�������ڶ�ʱ�����ݲ�һ�µ������ɾ��������������Ч��RamWriter���ύҪ��RamReader����reopen֮�������Ч
			 * ��ʱ���ڻ��������ɾ���ˣ���������������û�б��ֳ���
			 * */
			
			/* 3.�����������仯������Searcher */
			Future[] waitSearcher = new Future[1];
			core.getSearcher(true, false, waitSearcher);
			waitSearcher[0].get();
		} catch (IOException e) {
			throw new IOException("RealTimeUpdateHandler.commitAll() Error", e);
		} catch (InterruptedException e) {
			throw new IOException(e);
		} catch (ExecutionException e) {
			throw new IOException(e);
		}
	}

	protected void flushRamToDisk() throws IOException {
		File diskIndexDir = generateNextDiskDir();
		log.warn("[Flush-Index-File-To-Disk] ����������Ŀ¼·��Ϊ {" + diskIndexDir.getAbsolutePath() +"}");
		
		Directory newDiskDir = FSDirectory.open(diskIndexDir);
		
		final Directory oldRamDir = this.ramWriter.getDirectory();
		final IndexWriter oldRamWriter = this.ramWriter;
		final IndexReader oldRamReader = this.getRamReader();

		Utils.createSignFile(diskIndexDir);
		log.warn("[Flush-Index-File-To-Disk] ���������ļ�.");
		try {
			Directory.copy(this.ramWriter.getDirectory(), newDiskDir, false);
		} catch (IOException e) {
			log.error("[Flush-Index-File-To-Disk] COPY-ERROR!" ,e);
			throw e;
		}
 		Utils.deleteSignFile(diskIndexDir);

		TerminatorIndexReader newDiskReader = new TerminatorIndexReader(IndexReader.open(newDiskDir, false), true);
		getIndexReaderFactory().addDiskReader(newDiskReader);
		
		Directory newRamDir = new RAMDirectory();
		this.beforCreateNewRamIndexWriter();
		IndexWriter newRamWriter = this.createRAMIndexWriter(newRamDir);
		
		this.setRamWriter(newRamWriter);
		
		TerminatorIndexReader newRamReader = new TerminatorIndexReader(IndexReader.open(newRamDir,false),false);
		
		this.setRamReader(newRamReader);
		
		log.warn("[Flush-Index-File-To-Disk] SolrCore.getSearcher(true,false,null)");
		core.getSearcher(true, false, null);
		
		log.warn("[Flush-Index-File-To-Disk] �ͷ��ϵ��ڴ���������Դ.");
		oldRamDir.close();
		oldRamReader.close();
		oldRamWriter.close();
	}
	
	private synchronized File generateNextDiskDir() {
		File dataFileDir = new File(this.dataDir);
		
		int maxNum = 0;
		File[] diskIndexFiles = dataFileDir.listFiles();
		for(File file : diskIndexFiles) {
			if(file.isFile()) continue;
			
			String fileName = file.getName();
			String[] fileNameParts = fileName.split("_");
			
			if(fileNameParts.length == 2 && fileNameParts[0].equals("index") && Utils.isNumber(fileNameParts[1])) {
				int suffix = Integer.valueOf(fileNameParts[1]);
				if(suffix > maxNum) {
					maxNum = suffix;
				}
			}
		}
		
		File file = new File(this.dataDir,"index_" + (++maxNum));
		if(!file.exists()) {
			file.mkdir();
		}
		return file;
	}
	
	public void commit(CommitUpdateCommand cmd) throws IOException {
		this.ramWriter.commit();
		core.getSearcher(true, false, null);

		effectCount.set(0);
		lastCommitTime = System.currentTimeMillis();
	}

	protected void rollbackWriter() throws IOException {
		throw new UnsupportedOperationException("UnsupportedOperation - rollbackWriter()");
	}

	public void rollback(RollbackUpdateCommand cmd) throws IOException {
		throw new UnsupportedOperationException("UnsupportedOperation - rollbackWriter()");
	}

	public void close() throws IOException {
		if(this.getRamWriter() != null){
			this.getRamWriter().close();
		}
	}
	
	public void deleteByQuery(DeleteUpdateCommand cmd) throws IOException {
		throw new UnsupportedOperationException("UnsupportedOperation - deleteByQuery(DeleteUpdateCommand cmd)");
	}

	public int mergeIndexes(MergeIndexesCommand cmd) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	protected IndexReader getRamReader() {
		return this.getIndexReaderFactory().getRamReader();
	}

	protected TerminatorIndexReader getMainReader() {
		return this.getIndexReaderFactory().getMainReader();
	}
	        
	protected List<TerminatorIndexReader> getDiskReaders() {
		return this.getIndexReaderFactory().getDiskReaders();
	}
	
	private RealTimeIndexReaderFactory getIndexReaderFactory() {
		return (RealTimeIndexReaderFactory) (core.getIndexReaderFactory());
	}
	
	public void setRamWriter(IndexWriter newRamWriter){
		this.ramWriter = newRamWriter;
	}
	
	public void setRamReader(TerminatorIndexReader newRamReader) {
		this.getIndexReaderFactory().setRamReader(newRamReader);
	}

	public IndexWriter getRamWriter() {
		return this.ramWriter;
	}

	@Override
	public Category getCategory() {
		return null;
	}

	@Override
	public String getDescription() {
		return "RealTime UpdateHandler";
	}

	@Override
	public URL[] getDocs() {
		return null;
	}

	@Override
	public String getName() {
		return RealTimeUpdateHandler.class.getName();
	}

	@Override
	public String getSource() {
		return null;
	}

	@Override
	public String getSourceId() {
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public NamedList getStatistics() {
		NamedList<String> nl = new NamedList<String>();
		
		nl.add("Effect-Count:", this.effectCount.get() + "");
		
		nl.add("Last-Commit time:", new Date(this.lastCommitTime).toString());

		try {
			nl.add("Ram-Index maxDoc:", this.getRamReader().maxDoc() + "");
			nl.add("Ram-Index size:", (((RAMDirectory)(this.getRamWriter().getDirectory())).sizeInBytes() / (1024 * 1024)) + "MB");
		} catch (Exception e) {
			
		}
		
		nl.add("status { ", "isFlushing:"+ this.isFlushing + ", isCommiting:" + this.isCommiting + " }");
		return nl;
	}

	@Override
	public String getVersion() {
		return "1.0.0";
	}

	public String getDataDir() {
		return dataDir;
	}

	public void setDataDir(String dataDir) {
		this.dataDir = dataDir;
	}

	public AtomicInteger getEffectCount() {
		return effectCount;
	}

	public void setEffectCount(AtomicInteger effectCount) {
		this.effectCount = effectCount;
	}

	public long getLastCommitTime() {
		return lastCommitTime;
	}

	public void setLastCommitTime(long lastCommitTime) {
		this.lastCommitTime = lastCommitTime;
	}

	public int getCommitCountThreshold() {
		return commitCountThreshold;
	}

	public void setCommitCountThreshold(int commitCountThreshold) {
		this.commitCountThreshold = commitCountThreshold;
	}

	public int getCommitTimeThreshold() {
		return commitTimeThreshold;
	}

	public void setCommitTimeThreshold(int commitTimeThreshold) {
		this.commitTimeThreshold = commitTimeThreshold;
	}

	public int getFlushCountThreshold() {
		return flushCountThreshold;
	}

	public void setFlushCountThreshold(int flushCountThreshold) {
		this.flushCountThreshold = flushCountThreshold;
	}

	public int getFlushSizeThreshold() {
		return flushSizeThreshold;
	}

	public void setFlushSizeThreshold(int flushSizeThreshold) {
		this.flushSizeThreshold = flushSizeThreshold;
	}

	public CommitLogAccessor getCommitLogAccessor() {
		return commitLogAccessor;
	}

	public void setCommitLogAccessor(CommitLogAccessor commitLogAccessor) {
		this.commitLogAccessor = commitLogAccessor;
	}
}
