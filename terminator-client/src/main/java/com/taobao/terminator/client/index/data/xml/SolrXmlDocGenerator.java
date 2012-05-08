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
 * �����ݿ��ж�ȡ������ÿ������ת����һ��solr����ʶ���xml�ĵ��� ת��֮����������ݱ��ŵ�һ��String�з��ء�<br>
 * <code>mapToSolrXML</code>��������һ��isAllowDup�������ò�������ָ��
 * �¼���������Ƿ�Ҫ����֮ǰ�����ԡ�������ó�true����ôsolr�ڲ�������<br>
 * ֮ǰ�����uniqueKey����������Ƿ��Ѿ����ڸ�doc��������ڣ���ô�͸��ǡ�<br>
 * ������ó�false����ôsolr�ڲ���������ʱ�򲻻��uniqueKey���м�顣
 * 
 * ����������ȫ��������ʱ�򽫸ò������ó�false���Ա����Ч�ʡ���������ʱ�� �򽫸�ֵ��Ϊture��
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
	 * �������һ��Map�����е�����ת����Solr��ʶ���xml��ʽ��
	 * ����������������������޸�����������xml�ĵ�
	 * 
	 * @param row
	 *            ���ݿ��е�һ��
	 * @param overwrite
	 *            true��ʾupdate��false��ʾ����
	 * @return String��ʾ��xml�ĵ�
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
	 * ��������ɾ��������xml�ĵ�������unique keyɾ��
	 * @param value
	 * 	��solr��schemal.xml�ж����unique key��ֵ
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
	 * ��������ɾ��������xml�ĵ�������һ����ѯ��ɾ��
	 * @param value
	 * 	query������
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
				//���������ַ�
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
			throw new Exception("XMLStreamWriterʧ��",e);
		}
	}

	private void flush() throws Exception {
		try {
			xmlWriter.flush();
			bufferedWriter.flush();
			strWriter.flush();
		} catch (IOException e) {
			throw new Exception("ִ��StringWriter.flush()��������");
		} catch (XMLStreamException e) {
			throw new Exception("ִ��XMLStreamWriter.flush()��������",e);
		}
	}

	private void closeWriters() throws Exception {
		try {
			xmlWriter.close();
			bufferedWriter.close();
			strWriter.close();
		} catch (XMLStreamException e) {
			throw new Exception("�ر�XMLStreamWriter����");
		} catch (IOException e) {
			throw new Exception("�ر�StringWriter����",e);
		}
	}
}
