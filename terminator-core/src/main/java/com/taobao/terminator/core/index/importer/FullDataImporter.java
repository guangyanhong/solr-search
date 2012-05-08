package com.taobao.terminator.core.index.importer;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.solr.core.SolrCore;
import org.apache.solr.update.CommitUpdateCommand;

import com.taobao.terminator.core.index.IndexFileSearcher;
import com.taobao.terminator.core.util.MasterStatusCollector;

public class FullDataImporter extends AbstractDataImporter{

	public FullDataImporter(SolrCore solrCore, IndexFileSearcher fileSearcher) {
		super(solrCore, fileSearcher);
	}

	@Override
	public void importData() {
		int consumedFileNum = 0;
		while (true) {
			Collection<File> files = fileSearcher.listIndexFiles(null);
			
			if (files == null || files.size() == 0) {
				if(!MasterStatusCollector.get(solrCore.getName()).finished.get()){ //dump数据还没有结束，则继续等待数据写入
					if(logger.isDebugEnabled()){
						logger.debug("[Full-Data-Importer] 获取全量xml文件列表为空，稍等片刻继续取，因为还没有finished.");
					}
					try {
						Thread.sleep(2000);
						continue;
					} catch (InterruptedException e) {
						continue;
					}
				}else{ //dump数据已经结束了
					files = fileSearcher.listIndexFiles(null);
					if (files == null || files.size() == 0){
						logger.warn("[Full-Data-Importer] 全量文件全部消费完毕，且finished标志位为true,此次全量基本上就告一段落了吧.");
						break;
					}
				}
			}
			
			for (File file : files) {
				try {
					new IndexWriter(file,this.solrCore).writeIndex();
					consumedFileNum ++;
				} catch (Exception e) {
					logger.error("[Full-Data-Importer] 消费全量文件 [" + file.getAbsolutePath() + "]失败.",e);
				}finally{
					if(file.exists()){
						file.delete();
					}
				}
			}
		}
		
		if(consumedFileNum > 0){
			try {
				CommitUpdateCommand commitCmd = new CommitUpdateCommand(true);
				this.solrCore.getUpdateHandler().commit(commitCmd);
			} catch (IOException e) {
				logger.error("[Full-Data-Importer] 提交全量索引失败",e);
			}
		}
	}
}
