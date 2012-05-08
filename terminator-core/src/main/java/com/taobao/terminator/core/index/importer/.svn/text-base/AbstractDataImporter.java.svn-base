package com.taobao.terminator.core.index.importer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.SolrCore;

import com.taobao.terminator.core.index.IndexFileSearcher;


public abstract class AbstractDataImporter implements DataImporter{
	protected static Log logger = LogFactory.getLog(DataImporter.class);
	protected SolrCore solrCore = null;
	protected IndexFileSearcher fileSearcher = null;
	
	public AbstractDataImporter(SolrCore solrCore,IndexFileSearcher fileSearcher){
		this.solrCore = solrCore;
		this.fileSearcher = fileSearcher;
	}
	
	@Override
	public abstract void importData();
	
	/**
	 * 将XML文件Build进索引
	 * @author yusen
	 */
	protected class IndexWriter {
		private XMLStreamReader reader;
		private FileInputStream in;
		private SolrCore        solrCore;
		private File            indexFile;

		IndexWriter(File indexFile, SolrCore solrCore) throws FileNotFoundException, XMLStreamException, FactoryConfigurationError {
			logger.warn("要导入的xml文件名是：fileName:" + indexFile.getName());
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
				logger.error("索引xml格式错误！放入错误xml索引文件夹：" + this.solrCore.getCoreDescriptor().getCoreContainer().getSolrHome()+ File.separator + "ErrorXml", e);
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
					logger.error("将出错的文件xml文件导出时出错!", e);
				} catch (IOException e2) {
					logger.error("将出错的文件xml文件导出时出错!", e);
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
				logger.error("添加索引到solr失败！", e);
			} catch(Throwable e){
				logger.error(e,e);
			}finally {
				try {
					// 关闭XML流
					reader.close();
					// 关闭输入流
					in.close();
				} catch (XMLStreamException e) {
					logger.error("关闭XmlStream流失败", e);
				} catch (IOException e) {
					logger.error("关闭输入流失败", e);
				}
			}
		}
	}
}
