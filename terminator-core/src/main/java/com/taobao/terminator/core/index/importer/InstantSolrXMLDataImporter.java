package com.taobao.terminator.core.index.importer;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.SolrCore;

/**
 * 处理即时索引构建请求，每一个请求由一个线程来处理，保证调用能够快速返回。
 *
 * @author tianxiao
 */
@Deprecated
public class InstantSolrXMLDataImporter implements Runnable {
	private static Log logger = LogFactory.getLog(InstantSolrXMLDataImporter.class);
	private SolrCore core;
	private byte[] indexData;

	public InstantSolrXMLDataImporter(SolrCore core, byte[] indexData) {
		this.core = core;
		this.indexData = indexData;
	}

	/**
	 * 对输入的byte数组进行转化，时期符合XMLStreamReader对数据格式的要求
	 * @param indexData
	 */
	public void importIndex() {
		byte[] index;
		BufferedWriter bufferedWriter = null;
		OutputStreamWriter streamWriter = null;
		try {
			ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
			streamWriter = new OutputStreamWriter(dataStream, "UTF-8");
			bufferedWriter = new BufferedWriter(streamWriter);

			bufferedWriter.write("<docs>");
			bufferedWriter.write(new String(this.indexData));
			bufferedWriter.write("</docs>");
			
			bufferedWriter.flush();
			//将byte array中的数据复制出来
			index = dataStream.toByteArray();
			//调用buildIndex来构建索引
			this.buildIndex(index);
		} catch (IOException e) {
			logger.error("构建索引数据失败，忽略本次操作！", e);
		} finally{
			try {
				bufferedWriter.close();
			} catch (IOException e) {
				logger.error("关闭buffer writer失败！", e);
			}
			try {
				streamWriter.close();
			} catch (IOException e) {
				logger.error("关闭Output Stream Writer失败！", e);
			}
		}
	}

	/**
	 * 执行索引构建操作
	 * @param indexData
	 */
	private void buildIndex(byte[] indexData){
		ByteArrayInputStream stream = new ByteArrayInputStream(indexData);
		XMLStreamReader xmlStreamReader = null;
		try {
			xmlStreamReader = XMLInputFactory.newInstance().createXMLStreamReader(stream);
		} catch (Exception e) {
			logger.error("构建XMLReader失败！", e);
			return;
		}
		
		SolrCommandProcessor processor = new SolrCommandProcessor(xmlStreamReader, this.core.getSchema(), this.core);
		
		try {
			processor.process();
		} catch (XMLStreamException e) {
			logger.error("索引xml文档格式错误！記錄該文件...", e);
			
		} catch (IOException e) {
			logger.error("添加索引到solr失败！", e);
		} catch(Throwable e){
			logger.error(e,e);
		} finally{
			try{
				xmlStreamReader.close();
				stream.close();
			} catch (XMLStreamException e) {
				logger.error("关闭XmlStream流失败", e);
			} catch (IOException e) {
				logger.error("关闭输入流失败", e);
			}
		}
	}
	
	public void run() {
		this.importIndex();
	}
}
