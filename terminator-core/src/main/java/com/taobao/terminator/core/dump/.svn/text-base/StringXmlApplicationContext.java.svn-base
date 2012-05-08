package com.taobao.terminator.core.dump;

import java.io.UnsupportedEncodingException;

import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

public class StringXmlApplicationContext extends AbstractXmlApplicationContext {
	private String[] xmlStrings;

	public StringXmlApplicationContext(String xmlString) {
		super(null);
		this.setXmlStrings(new String[] { xmlString });
		refresh();
	}
	
	public StringXmlApplicationContext(String[] xmlStrings){
		super(null);
		this.setXmlStrings(xmlStrings);
		refresh();
	}

	@Override
	protected Resource[] getConfigResources() {
		Resource[] res = new Resource[xmlStrings.length];
		for (int i = 0; i < xmlStrings.length; i++) {
			try {
				res[i] = new ByteArrayResource(xmlStrings[i].getBytes("GBK"));
			} catch (UnsupportedEncodingException e) {
				return null;
			}

		}
		return res;
	}
	
	public String[] getXmlStrings() {
		return xmlStrings;
	}

	public void setXmlStrings(String[] xmlStrings) {
		this.xmlStrings = xmlStrings;
	}
}
