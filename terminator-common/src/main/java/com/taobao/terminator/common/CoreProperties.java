package com.taobao.terminator.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 对应配置文件  core.properties,每个core都有一个相应的配置文件，用于说明该机器下的该core的角色
 * 
 * @author yusen
 */
public class CoreProperties extends Properties{

	private static final long serialVersionUID = -4020028009909240962L;

	public CoreProperties() {
		super();
	}

	public CoreProperties(Properties defaults) {
		super(defaults);
	}
	
	public CoreProperties(File proFile) throws IOException{
		FileInputStream inputStream = new FileInputStream(proFile);
		this.load(inputStream);
	}
	
	public CoreProperties(InputStream inputStream) throws IOException{
		this.load(inputStream);
	}
	
	public boolean isMerger(){
		return Boolean.valueOf(this.getProperty("isMerger", "true"));
	}
	
	public boolean isWriter(){
		return Boolean.valueOf(this.getProperty("isWriter", "false"));
	}
	
	public boolean isReader(){
		return Boolean.valueOf(this.getProperty("isReader", "true"));
	}
	
	public void setMerger(boolean isMerger){
		this.setProperty("isMerger",isMerger?"true":"false");
	}
	
	public void setReader(boolean isReader){
		this.setProperty("isReader",isReader?"true":"false");
	}
	
	public void setWriter(boolean isWriter){
		this.setProperty("isWriter",isWriter?"true":"false");
	}
	
	public void setIP(String ip){
		this.setProperty("ip", ip);
	}
	
	public String getIP(){
		return this.getProperty("ip");
	}
	
	public void setPort(String port){
		this.setProperty("port", port);
	}
	
	public String getPort(){
		return this.getProperty("port");
	}
}
