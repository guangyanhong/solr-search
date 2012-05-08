package com.taobao.terminator.core.realtime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.IndexReaderFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexReader;

import com.taobao.terminator.core.realtime.common.IndexDirComparator;
import com.taobao.terminator.core.realtime.common.IndexDirFilter;

public class RealTimeIndexReaderFactory extends IndexReaderFactory{
	protected SolrCore solrCore;

	protected TerminatorIndexReader mainReader;
	protected List<TerminatorIndexReader> diskReaders;
	protected TerminatorIndexReader ramReader;
	
	protected AtomicBoolean isFirstCall = new AtomicBoolean(true);
	protected AtomicBoolean isAfterFull = new AtomicBoolean(false);
	
	protected List<File> sortFiles(File[] files){
		List<File> fl = new ArrayList<File>(files.length);
		for(File f : files) {
			fl.add(f);
		}
		Collections.sort(fl, new IndexDirComparator());
		return fl;
	}

	public IndexReader newReader(Directory indexDir, boolean readOnly) throws IOException {
		List<IndexReader> tmpReaders = new ArrayList<IndexReader>();
		//系统启动之初和全量完毕后第一次调用getSearcher方法的时候会走这个分支流程
		if(isFirstCall.getAndSet(false) || isAfterFull.getAndSet(false)) {
			IndexWriter.unlock(indexDir);
			
			this.mainReader = new TerminatorIndexReader(IndexReader.open(indexDir, null, false, termInfosIndexDivisor), true);
			
			tmpReaders.add(mainReader);
			
			if(indexDir instanceof FSDirectory) {
				File dataFile = ((FSDirectory)indexDir).getFile().getParentFile();
				
				File[] diskIndexDirs = dataFile.listFiles(new IndexDirFilter());
				
				if(diskIndexDirs != null && diskIndexDirs.length > 0){
					List<File> files = this.sortFiles(diskIndexDirs);
					
					this.diskReaders = new ArrayList<TerminatorIndexReader>(files.size());
					
					for(File file : files) {
						diskReaders.add(new TerminatorIndexReader(IndexReader.open(FSDirectory.open(file),false), true));
					}
					
					tmpReaders.addAll(this.diskReaders);
				}
			} else {
				throw new IOException("IndexDir is not instanceof FSDirectory!");
			}
		} else  { //正常运行的一般情况下走这个分支流程
			
			tmpReaders.add(mainReader);

			if (this.getDiskReaders() != null && !this.getDiskReaders().isEmpty()) {
				tmpReaders.addAll(getDiskReaders());
			}

			if (getRamReader() != null) {
				
				/* 
				 * 这个地方的Ram的IndexReader在reopen之后不能轻易关闭的，如果关闭的话，可能会的影响到正在使用之前这个IndexReader的搜索线程。
				 * 会出现 IndexReader has already closed的异常
				 * */
				TerminatorIndexReader newReader = getRamReader().reopenWithoutMapper();
				this.setRamReader(newReader);
				tmpReaders.add(this.getRamReader());
			}
		}
		
		TerminatorIndexReader[] readers = new TerminatorIndexReader[tmpReaders.size()];
		tmpReaders.toArray(readers);
		return new SolrIndexReader(new MultiReader(readers,false) {
			public Directory directory() {
				return mainReader.directory();
			}
			
			public long getVersion() {
				return mainReader.getVersion();
			}
		},null,0);
	}

	public void setRamReader(TerminatorIndexReader ramReader) {
		this.ramReader = ramReader;
	}
	
	public TerminatorIndexReader getRamReader() {
		return ramReader;
	}

	public TerminatorIndexReader getMainReader() {
		return mainReader;
	}
	
	public List<TerminatorIndexReader> getDiskReaders() {
		return this.diskReaders;
	}
	
	public void addDiskReader(TerminatorIndexReader newDiskReader) {
		if (diskReaders == null) {
			diskReaders = new ArrayList<TerminatorIndexReader>();
		}

		diskReaders.add(newDiskReader);
	}

	public AtomicBoolean getIsAfterFull() {
		return isAfterFull;
	}
	
	public NamedList<String> getStatistics() {
		NamedList<String> nl = new NamedList<String>();
		nl.add("disk index num", this.diskReaders.size() + "");
		return nl;
	}
}
