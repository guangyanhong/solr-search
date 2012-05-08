package com.taobao.terminator.client.index.data.xml;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import com.taobao.terminator.client.index.data.procesor.BoostDataProcessor;

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
	 * @param row
	 *            数据库中的一行
	 * @param overwrite
	 *            true表示update，false表示新增
	 * @return String表示的xml文档
	 * @throws Exception
	 */
	public String genSolrUpdateXML(Map<String,String> row, boolean overwrite) throws Exception {
		String result = null;
		this.initXMLWriter();
		try{
			this.solrDocGen(row, overwrite);
			this.flush();
			result = strWriter.toString();
		}finally{
			this.closeWriters();
		}
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
		String result = null;
		this.initXMLWriter();
		try{
			this.solrDelDocGen("id", value);
			this.flush();
			result = strWriter.toString();
		}finally{
			this.closeWriters();
		}
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
		String result = null;
		this.initXMLWriter();
		try{
			this.solrDelDocGen("query", query);
			this.flush();
			result = strWriter.toString();
		}finally{
			this.closeWriters();
		}
		return result;
	}
	
	private void solrDelDocGen(String mode, String  value) throws XMLStreamException{
		this.xmlWriter.writeStartElement("delete");
		this.xmlWriter.writeStartElement(mode);
		this.xmlWriter.writeCharacters(value);
		this.xmlWriter.writeEndElement();
		this.xmlWriter.writeEndElement();
	}
	
	public static void main(String[] args) throws Exception {
//		script&gt;alert(530535380);&lt;/script&gt
		SolrXmlDocGenerator gen = new SolrXmlDocGenerator();
		Map<String,String> row = new HashMap<String,String>();
		row.put("contet", "<script>alert('a')</script><script>alert('a')</script>");
		System.out.println(gen.genSolrUpdateXML(row, false));
	}
	
	private void solrDocGen(Map<String,String> row, boolean overwrite) throws XMLStreamException {
		this.xmlWriter.writeStartElement("add");
		this.xmlWriter.writeAttribute("overwrite", (overwrite ? "true" : "false"));
		this.xmlWriter.writeStartElement("doc");
		String docBoost = row.get(BoostDataProcessor.BOOST_NAME);
		try{
			Float.valueOf(docBoost);
		}catch(Exception e){
			docBoost = null;
		}
		
		row.remove(BoostDataProcessor.BOOST_NAME);
		
		this.xmlWriter.writeAttribute("boost",docBoost == null ? "1.0f" : docBoost);

		for (String key : row.keySet()) {
			String value = row.get(key);
			this.xmlWriter.writeStartElement("field");
			this.xmlWriter.writeAttribute("name", key);
			try{
				this.xmlWriter.writeCharacters(value == null ? "null" : value);
			}catch(Exception e){
				//处理特殊字符
				this.xmlWriter.writeCharacters(value == null ? "null" : stripNonValidXMLCharacters(value));
			}
			this.xmlWriter.writeEndElement();
		}

		this.xmlWriter.writeEndElement();
		this.xmlWriter.writeEndElement();
	}
	
	public static String stripNonValidXMLCharacters(String in) {
	    StringBuffer out = new StringBuffer(); 
	    char current; 

	    if (in == null || ("".equals(in)))
	        return ""; 
	    for (int i = 0; i < in.length(); i++) {
	        current = in.charAt(i);             
	        if ((current == 0x9) || (current == 0xA) || (current == 0xD)
	                || ((current >= 0x20) && (current <= 0xD7FF))
	                || ((current >= 0xE000) && (current <= 0xFFFD))
	                || ((current >= 0x10000) && (current <= 0x10FFFF)))
	            out.append(current);
	    }
	    return out.toString();
	}
	
	private void initXMLWriter() throws Exception {
		try {
			this.strWriter = new StringWriter();
			this.bufferedWriter = new BufferedWriter(this.strWriter);
			this.xmlWriter = xmlWriterFactory.createXMLStreamWriter(this.bufferedWriter);
		} catch (XMLStreamException e) {
			throw new Exception("XMLStreamWriter失败",e);
		}
	}

	private void flush() throws Exception {
		try {
			xmlWriter.flush();
			bufferedWriter.flush();
			strWriter.flush();
		} catch (IOException e) {
			throw new Exception("执行StringWriter.flush()方法出错！");
		} catch (XMLStreamException e) {
			throw new Exception("执行XMLStreamWriter.flush()方法出错！",e);
		}
	}

	private void closeWriters() throws Exception {
		try {
			xmlWriter.close();
			bufferedWriter.close();
			strWriter.close();
		} catch (XMLStreamException e) {
			throw new Exception("关闭XMLStreamWriter出错！");
		} catch (IOException e) {
			throw new Exception("关闭StringWriter出错！",e);
		}
	}
}
