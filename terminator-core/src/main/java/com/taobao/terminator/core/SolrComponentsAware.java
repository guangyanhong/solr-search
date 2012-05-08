package com.taobao.terminator.core;

import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * Solr的**Aware接口的集合
 * 
 * @author yusen
 */
public interface SolrComponentsAware extends SolrCoreAware,ResourceLoaderAware,SolrInfoMBean{

}
