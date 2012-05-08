package com.taobao.terminator.core.realtime;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.solr.core.DirectoryFactory;

public class TerminatorDirectoryFactory extends DirectoryFactory{

	@Override
	public Directory open(String path) throws IOException {
		return FSDirectory.open(new File(path),new SimpleFSLockFactory(path));
	}
}
