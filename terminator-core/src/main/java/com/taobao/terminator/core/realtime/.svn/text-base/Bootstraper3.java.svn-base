package com.taobao.terminator.core.realtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * 整个实时索引的主启动流程控制
 * 
 * @author yusen
 */
public class Bootstraper3 implements NamedListInitializedPlugin {
	private final Log log = LogFactory.getLog(Bootstraper3.class);
	
	private CoreContainer     coreContainer;
	private String            coreName;
	private LeaderContainer   leaderContainer;
	private FollowerContainer followerContainer;
	private boolean           isLeader = false;
	
	public Bootstraper3(SolrCore solrCore) {
		coreContainer = solrCore.getCoreDescriptor().getCoreContainer();
		coreName = solrCore.getName();
	}

	@SuppressWarnings("unchecked")
	public void init(NamedList args) {
		SolrConfig config = coreContainer.getCore(this.coreName).getSolrConfig();
		this.isLeader = config.getBool("terminatorService/isLeader",false);
		log.warn("LocalHost is <<<<< " + (this.isLeader ? "LEADER" : "FOLLOWER") + " >>>>>");
		if (this.isLeader) {
			this.createLeaderContainer();
		} else {
			this.createFollowerContainer();
		}
	}

	private void createLeaderContainer() {
		this.leaderContainer = new LeaderContainer(coreContainer,coreName);
	}

	private void createFollowerContainer() {
		this.followerContainer = new FollowerContainer(coreContainer,coreName);
	}
	
	public boolean isLeader() {
		return isLeader;
	}

	public CoreContainer getCoreContainer() {
		return coreContainer;
	}

	public String getCoreName() {
		return coreName;
	}

	public LeaderContainer getLeaderContainer() {
		return leaderContainer;
	}

	public FollowerContainer getFollowerContainer() {
		return followerContainer;
	}
	
	public SolrCore getSolrCore() {
		return this.coreContainer.getCore(this.getCoreName());
	}
}