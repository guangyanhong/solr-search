package com.taobao.terminator.core;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 将数据库中读取出来的每行数据转换成一个solr可以识别的xml文档， 转换之后的所有数据被放到一个String中返回。<br>
 * <code>mapToSolrXML</code>方法接受一个isAllowDup参数，该参数用来指明
 * 新加入的索引是否要覆盖之前的所以。如果设置成true，那么solr在插入索引<br>
 * 之前会根据uniqueKey检查索引中是否已经存在该doc，如果存在，那么就覆盖。<br>
 * 如果设置成false，那么solr在插入索引的时候不会对uniqueKey进行检查。
 * 
 * 建议在生成全量索引的时候将该参数设置成false，以便提高效率。在增量的时候 则将该值设为ture。
 * 
 * @author tianxiao
 * 
 */
public class SolrXmlDocGenerator {
	private static XMLOutputFactory xmlWriterFactory;
	private static Log logger = LogFactory.getLog(SolrXmlDocGenerator.class);

	private XMLStreamWriter xmlWriter;
	private StringWriter strWriter;
	private BufferedWriter bufferedWriter;

	static {
		xmlWriterFactory = XMLOutputFactory.newInstance();
	}

	/**
	 * 将传入的一个Map数组中的数据转换成Solr可识别的xml格式。
	 * 这个方法用来生成新增和修改索引操作的xml文档
	 * 
	 * @param aRow
	 *            数据库中的一行
	 * @param isAllowDup
	 *            true表示update，false表示新增
	 * @return String表示的xml文档
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public String mapToSolrUpdateXML(Map aRow, boolean isAllowDup)
			throws Exception {
		this.initXMLWriter();
		
		this.solrUpdateDocGen(aRow, isAllowDup);

		this.flush();
		
		String result = strWriter.toString();

		this.closeWriters();

		return result;
	}

	/**
	 * 用来生成删除操作的xml文档，根据unique key删除
	 * @param value
	 * 	在solr的schemal.xml中定义的unique key的值
	 * @return
	 * @throws Exception 
	 */
	public String genSolrDeleteXMLByUniqueKey(String value) throws Exception{
		this.initXMLWriter();
		
		this.solrDelDocGen("id", value);
		
		this.flush();
		
		String result = strWriter.toString();
		
		this.closeWriters();
		return result;
	}
	
	/**
	 * 用来生成删除操作的xml文档，根据一个查询来删除
	 * @param value
	 * 	query的内容
	 * @return
	 * @throws Exception
	 */
	public String genSolrDeleteXMLByQuery(String query) throws Exception{
		this.initXMLWriter();
		
		this.solrDelDocGen("query", query);
		
		this.flush();
		
		String result = strWriter.toString();
		
		this.closeWriters();
		return result;
	}
	
	private void solrDelDocGen(String mode, String  value) throws XMLStreamException{
		this.xmlWriter.writeStartElement("delete");
		
		this.xmlWriter.writeStartElement(mode);
		
		this.xmlWriter.writeCharacters(value);
		
		this.xmlWriter.writeEndElement();
		
		this.xmlWriter.writeEndElement();
	}
	
	@SuppressWarnings("unchecked")
	private void solrUpdateDocGen(Map aRow, boolean isAllowDup) throws XMLStreamException {
		this.xmlWriter.writeStartElement("add");
		if (!isAllowDup) { // 如果允许重复，那么就将overwrite属性设为false，solr对属性的默认值是true
			this.xmlWriter.writeAttribute("overwrite", "false");
		}
		this.xmlWriter.writeStartElement("doc");

		for (Object key : aRow.keySet()) {
			Object value = aRow.get(key);
			this.xmlWriter.writeStartElement("field");
			this.xmlWriter.writeAttribute("name", key.toString());
			this.xmlWriter.writeCharacters(value == null ? "" : value.toString());
			this.xmlWriter.writeEndElement();
		}

		this.xmlWriter.writeEndElement();
		this.xmlWriter.writeEndElement();
	}

	
	private void initXMLWriter() throws Exception {
		try {
			this.strWriter = new StringWriter();
			this.bufferedWriter = new BufferedWriter(this.strWriter);
			this.xmlWriter = xmlWriterFactory
					.createXMLStreamWriter(this.bufferedWriter);
			this.xmlWriter = xmlWriterFactory.createXMLStreamWriter(this.strWriter);
		} catch (XMLStreamException e) {
			logger.error("XMLStreamWriter失败！", e);
			throw new Exception("XMLStreamWriter失败！");
		}
	}

	private void flush() throws Exception {
		try {
			xmlWriter.flush();
			bufferedWriter.flush();
			strWriter.flush();
		} catch (IOException e) {
			logger.error("执行StringWriter.flush()方法出错！", e);
			throw new Exception("执行StringWriter.flush()方法出错！");
		} catch (XMLStreamException e) {
			logger.error("执行XMLStreamWriter.flush()方法出错！", e);
			throw new Exception("执行XMLStreamWriter.flush()方法出错！");
		}
	}

	private void closeWriters() throws Exception {
		try {
			xmlWriter.close();
			bufferedWriter.close();
			strWriter.close();
		} catch (XMLStreamException e) {
			logger.error("关闭XMLStreamWriter出错！", e);
			throw new Exception("关闭XMLStreamWriter出错！");
		} catch (IOException e) {
			logger.error("关闭StringWriter出错！", e);
			throw new Exception("关闭StringWriter出错！");
		}
	}
}
