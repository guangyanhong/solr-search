package com.taobao.terminator.web;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.NoInitialContextException;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.servlet.SolrDispatchFilter;

import com.taobao.terminator.common.zk.NodeLifeManager;
import com.taobao.terminator.common.zk.OnReconnect;
import com.taobao.terminator.common.zk.TerminatorZKException;
import com.taobao.terminator.common.zk.TerminatorZkClient;
import com.taobao.terminator.core.ConfigControllor;
import com.taobao.terminator.core.SolrConfigGenerator;
import com.taobao.terminator.core.service.MultiServiceContainer;
import com.taobao.terminator.core.service.ZkClientHolder;
import com.taobao.terminator.core.wachers.CoreWatcher;
import com.taobao.terminator.core.wachers.MultiConfigWatcher;
import com.taobao.terminator.core.wachers.NodeWatcher;
import com.taobao.terminator.core.wachers.TerminatorWatcher;

/**
 * @author yusen(lishuai)
 * @since 1.0, 2009-12-17 下午01:41:33
 **/

public class XSolrDispatchFilter extends SolrDispatchFilter {

	protected static Log log = LogFactory.getLog(XSolrDispatchFilter.class);

	public void superInit(FilterConfig filterConfig) throws ServletException {
		super.init(filterConfig);
	}
	
	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		try {
			String solrHome = SolrResourceLoader.locateSolrHome();
			
			TerminatorZkClient zkClient = ZkClientBuilder.buildZkClient();
			
			//控制可以本地启动，无需从zk抓取配置文件，以本地配置文件为准，本地是什么就是什么.
			String noZkString  = System.getProperty("noZk");
			
			TerminatorWatcher nodeWatcher = new NodeWatcher(zkClient);
			TerminatorWatcher coreWatcher = new CoreWatcher(zkClient);
			TerminatorWatcher multiConfigWatcher = new MultiConfigWatcher(zkClient);
			
			ConfigControllor configPuller = new ConfigControllor(zkClient,nodeWatcher,coreWatcher,multiConfigWatcher);
		
			if(noZkString == null || noZkString.equalsIgnoreCase("false")){
				configPuller.fetchConf(true);
				SolrConfigGenerator solrConfigGen = new SolrConfigGenerator(solrHome);
				solrConfigGen.generateSolrXml();
			}
			
			final NodeLifeManager nodeLifeManager = new NodeLifeManager(zkClient);
			nodeLifeManager.registerSelf();
			
			zkClient.setOnReconnect(new OnReconnect() {
				@Override
				public void onReconnect(TerminatorZkClient zkClient) throws Exception{
					nodeLifeManager.setZkClient(zkClient);
					boolean isAlive = false;
					try{
						isAlive = nodeLifeManager.isAlive();
					}catch(Exception e){
						log.error("", e);
					}
					if(!isAlive){
						nodeLifeManager.registerSelf();
					}
				}
			});
			
			//Solr启动
			this.superInit(filterConfig);
			
			nodeWatcher.setCoreContainer(cores);
			coreWatcher.setCoreContainer(cores);
			multiConfigWatcher.setCoreContainer(cores);
			
			ZkClientHolder.zkClient = zkClient;
			//初始化TermnatorService
			MultiServiceContainer.createInstance(cores);

		} catch (Throwable e) {
			log.error("Error! start XSolrDispatchFilter", e);
			throw new ServletException("Error! start XSolrDispatchFilter", e);
		}
	}
	
	public static class ZkClientBuilder {
		
		public static TerminatorZkClient buildZkClient() throws IOException, TerminatorZKException{ 
			String zkConfigPath = getZkConfigPath();
			File confFile = null;
			if(StringUtils.isNotBlank(zkConfigPath)){
				confFile = new File(zkConfigPath);
			}else{
				String solrHome = SolrResourceLoader.locateSolrHome();
				confFile = new File(solrHome,"zkconf.properties");
			}
			
			if(confFile.exists()){
				InputStream input = new FileInputStream(confFile);
				Properties zkPro = new Properties();
				zkPro.load(input);
				String zkAddress = zkPro.getProperty("zkAddress");
				String timeoutString = zkPro.getProperty("zkClientTimeout");
				int timeout = Integer.valueOf(timeoutString);
				
				TerminatorZkClient zkClient = new TerminatorZkClient(zkAddress, timeout,null,false);
				return zkClient;
			}else{
				throw new FileNotFoundException("Can not found the zk-config file,File path should be {" + confFile.getPath() +"}");
			}
		}
		
		public static String getZkConfigPath() {
		    String home = null;
		    try {
		      Context c = new InitialContext();
		      home = (String)c.lookup("java:comp/env/zkconfig/path");
		      log.info("Using JNDI solr.home: "+home );
		    } catch (NoInitialContextException e) {
		      log.info("JNDI not configured for java:comp/env/zkconfig/path (NoInitialContextEx)");
		    } catch (NamingException e) {
		      log.info("No /java:comp/env/zkconfig/path in JNDI");
		    } catch( RuntimeException ex ) {
		      log.warn("Odd RuntimeException while testing for JNDI: " + ex.getMessage());
		    } 
		    
		    return home;
	}
}}
