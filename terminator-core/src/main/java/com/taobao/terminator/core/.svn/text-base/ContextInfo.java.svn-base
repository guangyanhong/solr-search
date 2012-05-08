package com.taobao.terminator.core;

import java.util.HashMap;

import org.apache.solr.core.SolrCore;

public class ContextInfo extends HashMap<String,Object>{
	private static final long serialVersionUID = -6240536700010709214L;
	
	public static final String CORENAME 	= "coreName";
	public static final String INSTANCEPATH = "instancePath";
	public static final String CONFPATH 	= "confPath";
	public static final String DATAPATH 	= "dataPath";
	public static final String INDEXPATH	= "indexPath";
	public static final String SOLRCORE 	= "solrCore";
	
	private SolrCore solrCore;
	
	public ContextInfo(SolrCore solrCore){
		super();
		this.solrCore = solrCore;
		this.initContext();
	}
	
	private void initContext(){
		this.put(SOLRCORE, solrCore);
		this.put(CORENAME, solrCore.getName());
		this.put(DATAPATH, solrCore.getResourceLoader().getDataDir());
		this.put(INDEXPATH, solrCore.getIndexDir());
		this.put(CONFPATH, solrCore.getResourceLoader().getConfigDir());
		this.put(INSTANCEPATH, solrCore.getResourceLoader().getInstanceDir());
	}
}
