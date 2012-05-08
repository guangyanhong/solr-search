/*package com.taobao.terminator.core.realtime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.IndexReaderFactory;
import org.apache.solr.core.SolrCore;

import com.taobao.terminator.core.realtime.common.IndexDirComparator;
import com.taobao.terminator.core.realtime.common.IndexDirFilter;

public class RealTimeIndexReaderFactory extends IndexReaderFactory{
	protected SolrCore solrCore;

	protected TerminatorIndexReader mainReader;
	protected List<TerminatorIndexReader> diskReaders;
	protected IndexReader ramReader;
	
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
		
		//ϵͳ����֮����ȫ����Ϻ��һ�ε���getSearcher������ʱ����������֧����
		if(isFirstCall.getAndSet(false) || isAfterFull.getAndSet(false)) {
			this.mainReader = new TerminatorIndexReader(IndexReader.open(indexDir, null, false, termInfosIndexDivisor), null);
			tmpReaders.add(mainReader);
			
			if(indexDir instanceof FSDirectory) {
				File dataFile = ((FSDirectory)indexDir).getFile().getParentFile();
				
				File[] diskIndexDirs = dataFile.listFiles(new IndexDirFilter());
				
				if(diskIndexDirs != null && diskIndexDirs.length > 0){
					List<File> files = this.sortFiles(diskIndexDirs);
					
					this.diskReaders = new ArrayList<TerminatorIndexReader>(files.size());
					
					for(File file : files) {
						diskReaders.add(new TerminatorIndexReader(IndexReader.open(FSDirectory.open(file),false), null));
					}
					
					tmpReaders.addAll(this.diskReaders);
				}
			} else {
				throw new IOException("IndexDir is not instanceof FSDirectory!");
			}
		} else  { //�������е�һ��������������֧����
			
			tmpReaders.add(mainReader);

			if (this.getDiskReaders() != null && !this.getDiskReaders().isEmpty()) {
				tmpReaders.addAll(getDiskReaders());
			}

			if (getRamReader() != null) {
				IndexReader oldReader = ramReader;
				IndexReader newReader = ramReader.reopen();
				if (newReader != ramReader) {
					oldReader.close();
					this.setRamReader(newReader);
				}
				tmpReaders.add(this.getRamReader());
			}
		}
		
		IndexReader[] readers = new IndexReader[tmpReaders.size()];
		tmpReaders.toArray(readers);
		return new MultiReader(readers,false) {
			public Directory directory() {
				return mainReader.directory();
			}
			
			public long getVersion() {
				return mainReader.getVersion();
			}
		};
	}

	public void setRamReader(IndexReader ramReader) {
		this.ramReader = ramReader;
	}
	
	public IndexReader getRamReader() {
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
*/