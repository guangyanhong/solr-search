/*package com.taobao.terminator.core.index.importer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.CommitUpdateCommand;

import com.taobao.terminator.common.constant.IndexEnum;
import com.taobao.terminator.common.constant.IndexType;
import com.taobao.terminator.core.index.IndexFileSearcher;
import com.taobao.terminator.core.util.IndexFileUtils;
import com.taobao.terminator.core.util.MasterStatusCollector;

*//**
 * <code>SolrXMLDataImporter</code>扫描一个指定的目录，并将该目录下面的所有索引xml文件
 * 读进来，然后将包含在这些文件中的索引信息导入到Solr中
 * 
 * @author tianxiao
 * 
 *//*
public class SolrXMLDataImporter {
	private static Log logger = LogFactory.getLog(SolrXMLDataImporter.class);

	private SolrCore          core;
	private IndexFileSearcher fileSearcher;
	private AtomicBoolean     isRunning = new AtomicBoolean(false);
 	private IndexType         type;

	public SolrXMLDataImporter(SolrCore core,  IndexType type) {
		this.core      = core;
		this.isRunning = new AtomicBoolean(false);
		this.type      = type;
	}
	
	public void importData() {
		new InternalImporter(this.core).importData();
	}

	private void fullImport(){
		Set<String> badFiles = new HashSet<String>();
		isRunning.getAndSet(true);
		logger.warn(Thread.currentThread().getName() + "开始" + SolrXMLDataImporter.this.type.getValue() + "索引的导入。。。");
		Collection<File> files = null;
		
		while (true) {
			{
				if (type == IndexType.FULL) {
					files = fileSearcher.listIndexFiles(badFiles);
					if (files == null || files.size() == 0) {
						if(!MasterStatusCollector.get(core.getName()).finished.get()){ //dump数据还没有结束，则继续等待数据写入
							try {
								logger.debug("全量索引任务正在消费，但是文件夹中没有索引数据文件，等待文件写入.");
								Thread.sleep(2000);
								continue;
							} catch (InterruptedException e) {
								logger.error(type + "------在" + "等待下一次扫描文件夹的时候被中断，重新开始。" + Thread.currentThread().getName() + " is interrupted.");
								continue;
							}
						}else{ //dump数据已经结束了
							break;
						}
					}
				} else if (type == IndexType.INCREMENT) {
					files = fileSearcher.listIndexFiles(badFiles);
					if (files == null || files.size() == 0) {
						break;
					}
				}
			}
			
			logger.warn("共获取到文件<" + files.size() + ">个");
			logger.warn("当前badFiles中的文件数为<" + badFiles.size() + ">，badFiles:" + badFiles.toString());
			
			
			for (File file : files) {
				if (!badFiles.contains(file.getName())) {
					try {
						new IndexWriter(file,this.core).writeIndex();
					} catch (Exception e) {
						SolrXMLDataImporter.logger.error(SolrXMLDataImporter.this.type + "------" + "创建写索引任务失败，该任务对应的文件为:" + file.getName(), e);
						if (!file.renameTo(new File(file.getAbsolutePath() + File.separator + file.getName() + ".del"))) {
							badFiles.add(file.getName());
						}
					}
					if (type == IndexType.INCREMENT) { //增量消费的话，每消费一个文件记录一下时间文件
						String fileName = file.getName().substring(0, file.getName().lastIndexOf("."));
						IndexFileUtils.writeIncrStartTimeToFile(fileSearcher.getRootDir(),fileName);
					}
				}
			}

			if (type.equals(IndexType.FULL)) {
				for (File aFile : files) {
					try {
						if (aFile.getName().contains(IndexEnum.INDEX_FILE_SUFFIX.getValue())) {
							FileUtils.forceDelete(aFile);
						}
					} catch (IOException e) {
						logger.error("删除文件失败，尝试重命名文件，file:"+ aFile.getAbsolutePath(), e);
						if (!aFile.renameTo(new File(aFile.getAbsolutePath() + File.separator + aFile.getName() + ".del"))) {
							badFiles.add(aFile.getName());
						}
					}
				}
			
			}else if (type.equals(IndexType.INCREMENT)) {
				break;
			}
		}
		
		try {
			boolean isOptimize = SolrXMLDataImporter.this.type == IndexType.FULL;

			SolrXMLDataImporter.logger.warn(type + "------" + Thread.currentThread().getName() + "索引导入完成，提交索引，optimize=" + isOptimize);
			CommitUpdateCommand commitCmd = new CommitUpdateCommand(isOptimize);
			this.core.getUpdateHandler().commit(commitCmd);
		} catch (IOException e) {
			SolrXMLDataImporter.logger.error(SolrXMLDataImporter.this.type + "------" + "Commit索引失败。。。", e);
		}
		isRunning.getAndSet(false);
	
	}
	
	private class InternalImporter {
		private SolrCore core;
		Set<String> badFiles;

		public InternalImporter(SolrCore core) {
			this.core = core;
			this.badFiles = new HashSet<String>();
		}

		private void cleanUp(Collection<File> files) {
			for (File aFile : files) {
				try {
					if (aFile.getName().contains(IndexEnum.INDEX_FILE_SUFFIX.getValue())) {
						FileUtils.forceDelete(aFile);
					}
				} catch (IOException e) {
					logger.error("删除文件失败，尝试重命名文件，file:"+ aFile.getAbsolutePath(), e);
					if (!aFile.renameTo(new File(aFile.getAbsolutePath() + File.separator + aFile.getName() + ".del"))) {
						badFiles.add(aFile.getName());
					}
				}
			}
		}

		public void importData() {
			isRunning.getAndSet(true);
			logger.warn(Thread.currentThread().getName() + "开始" + SolrXMLDataImporter.this.type.getValue() + "索引的导入。。。");
			Collection<File> files = null;
			
			while (true) {
				{
					if (type == IndexType.FULL) {
						files = fileSearcher.listIndexFiles(this.badFiles);
						if (files == null || files.size() == 0) {
							if(!MasterStatusCollector.get(core.getName()).finished.get()){ //dump数据还没有结束，则继续等待数据写入
								try {
									logger.debug("全量索引任务正在消费，但是文件夹中没有索引数据文件，等待文件写入.");
									Thread.sleep(2000);
									continue;
								} catch (InterruptedException e) {
									logger.error(type + "------在" + "等待下一次扫描文件夹的时候被中断，重新开始。" + Thread.currentThread().getName() + " is interrupted.");
									continue;
								}
							}else{ //dump数据已经结束了
								break;
							}
						}
					} else if (type == IndexType.INCREMENT) {
						files = fileSearcher.listIndexFiles(this.badFiles);
						if (files == null || files.size() == 0) {
							break;
						}
					}
				}
				
				logger.warn("共获取到文件<" + files.size() + ">个");
				logger.warn("当前badFiles中的文件数为<" + badFiles.size() + ">，badFiles:" + badFiles.toString());
				
				
				for (File file : files) {
					if (!this.badFiles.contains(file.getName())) {
						try {
							new IndexWriter(file,this.core).writeIndex();
						} catch (Exception e) {
							SolrXMLDataImporter.logger.error(SolrXMLDataImporter.this.type + "------" + "创建写索引任务失败，该任务对应的文件为:" + file.getName(), e);
							if (!file.renameTo(new File(file.getAbsolutePath() + File.separator + file.getName() + ".del"))) {
								badFiles.add(file.getName());
							}
						}
						if (type == IndexType.INCREMENT) { //增量消费的话，每消费一个文件记录一下时间文件
							String fileName = file.getName().substring(0, file.getName().lastIndexOf("."));
							IndexFileUtils.writeIncrStartTimeToFile(fileSearcher.getRootDir(),fileName);
						}
					}
				}

				if (type.equals(IndexType.FULL)) {
					this.cleanUp(files);
				}else if (type.equals(IndexType.INCREMENT)) {
					break;
				}
			}
			
			try {
				boolean isOptimize = SolrXMLDataImporter.this.type == IndexType.FULL;

				SolrXMLDataImporter.logger.warn(type + "------" + Thread.currentThread().getName() + "索引导入完成，提交索引，optimize=" + isOptimize);
				CommitUpdateCommand commitCmd = new CommitUpdateCommand(isOptimize);
				this.core.getUpdateHandler().commit(commitCmd);
			} catch (IOException e) {
				SolrXMLDataImporter.logger.error(SolrXMLDataImporter.this.type + "------" + "Commit索引失败。。。", e);
			}
			isRunning.getAndSet(false);
		}
	}
	
	private class IndexWriter {
		private XMLStreamReader reader;
		private FileInputStream in;
		private SolrCore        solrCore;
		private File            indexFile;

		IndexWriter(File indexFile, SolrCore solrCore) throws FileNotFoundException, XMLStreamException, FactoryConfigurationError {
			SolrXMLDataImporter.logger.warn("要导入的xml文件名是：fileName:" + indexFile.getName());
			in = new FileInputStream(indexFile);
			this.reader = this.initXmlReader(in);
			this.solrCore = solrCore;
			this.indexFile = indexFile;
		}

		private XMLStreamReader initXmlReader(InputStream in) throws XMLStreamException, FactoryConfigurationError,FileNotFoundException {
			return XMLInputFactory.newInstance().createXMLStreamReader(in);
		}

		public void writeIndex() {
			SolrCommandProcessor processor = new SolrCommandProcessor(reader, this.solrCore.getSchema(), this.solrCore);
			try {
				processor.process();
			} catch (XMLStreamException e) {
				SolrXMLDataImporter.logger.error("索引xml格式错误！放入错误xml索引文件夹：" + this.solrCore.getCoreDescriptor().getCoreContainer().getSolrHome()+ File.separator + "ErrorXml", e);
				File errorXmlDir = new File(this.solrCore.getCoreDescriptor().getCoreContainer().getSolrHome() + File.separator + "ErrorXml");
			
				if (!errorXmlDir.exists()) {
					errorXmlDir.mkdir();
				}

				FileChannel fcout = null;
				FileChannel fcin  = null;
				FileOutputStream fout = null;
				try {

					File errorXml = new File(errorXmlDir, this.indexFile.getName());
					fout = new FileOutputStream(errorXml);
					fcout = fout.getChannel();
					fcin = in.getChannel();
					fcin.transferTo(0, fcin.size(), fcout);

					
				} catch (FileNotFoundException e1) {
					SolrXMLDataImporter.logger.error("将出错的文件xml文件导出时出错!", e);
				} catch (IOException e2) {
					SolrXMLDataImporter.logger.error("将出错的文件xml文件导出时出错!", e);
				}finally{
					if(fout != null)
						try {
							fcout.close();
						} catch (IOException e1) {
							logger.error("关闭FileChannel失败",e);
						}
					if(fcin != null)
						try {
							fcin.close();
						} catch (IOException e1) {
							logger.error("关闭FileChannel失败",e);
						}
					if(fout != null)
						try {
							fout.close();
						} catch (IOException e1) {
							logger.error("关闭FileOutputStream失败",e);
						}
				}
			
			} catch (IOException e) {
				SolrXMLDataImporter.logger.error("添加索引到solr失败！", e);
			} catch(Throwable e){
				logger.error(e,e);
			}finally {
				try {
					// 关闭XML流
					reader.close();
					// 关闭输入流
					in.close();
				} catch (XMLStreamException e) {
					SolrXMLDataImporter.logger.error("关闭XmlStream流失败", e);
				} catch (IOException e) {
					SolrXMLDataImporter.logger.error("关闭输入流失败", e);
				}
			}
		}
	}


	public boolean isRunning() {
		return this.isRunning.get();
	}

	public SolrCore getCore() {
		return core;
	}

	public void setCore(SolrCore core) {
		this.core = core;
	}

	public IndexFileSearcher getFileSearcher() {
		return fileSearcher;
	}

	public void setFileSearcher(IndexFileSearcher fileSearcher) {
		this.fileSearcher = fileSearcher;
	}
}
*/