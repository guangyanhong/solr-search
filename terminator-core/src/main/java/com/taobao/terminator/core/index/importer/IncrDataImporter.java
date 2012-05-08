package com.taobao.terminator.core.index.importer;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.solr.core.SolrCore;
import org.apache.solr.update.CommitUpdateCommand;

import com.taobao.terminator.core.index.IndexFileSearcher;
import com.taobao.terminator.core.util.IndexFileUtils;

public class IncrDataImporter extends AbstractDataImporter {

	public IncrDataImporter(SolrCore solrCore, IndexFileSearcher fileSearcher) {
		super(solrCore, fileSearcher);
	}

	@Override
	public void importData() {
		int consumedFileNum = 0;
		while(true){
			Collection<File> files = fileSearcher.listIndexFiles(null);
			if(files == null || files.isEmpty()){
				break;
			}
			
			for (File file : files) {
				try {
					new IndexWriter(file,this.solrCore).writeIndex();
					consumedFileNum ++ ;
				} catch (Exception e) {
					logger.error("[Incr-Data-Importer] ��������XML�ļ� [" + file.getAbsolutePath() + "]ʧ��,ɾ��֮.",e);
					if(file.exists()){
						file.delete();
					}
				} finally{
					String fileName = file.getName().substring(0, file.getName().lastIndexOf("."));
					IndexFileUtils.writeIncrStartTimeToFile(fileSearcher.getRootDir(),fileName);
				}
			}
		}

		//һ���ļ���û�����������ύ
		if(consumedFileNum > 0){
			try {
				CommitUpdateCommand commitCmd = new CommitUpdateCommand(false);
				this.solrCore.getUpdateHandler().commit(commitCmd);
			} catch (IOException e) {
				logger.error("[Incr-Data-Importer] �ύ����������ʧ��",e);
			}
		}
	}

}
