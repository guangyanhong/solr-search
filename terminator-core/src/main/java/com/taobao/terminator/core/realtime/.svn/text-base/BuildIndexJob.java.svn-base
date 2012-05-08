package com.taobao.terminator.core.realtime;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.UpdateHandler;

import com.taobao.terminator.common.perfutil.PerfTracer;
import com.taobao.terminator.common.protocol.AddDocumentRequest;
import com.taobao.terminator.common.protocol.DeleteByIdRequest;
import com.taobao.terminator.common.protocol.DeleteByQueryRequest;
import com.taobao.terminator.common.protocol.UpdateDocumentRequest;
import com.taobao.terminator.core.dump.SolrCommandBuilder;
import com.taobao.terminator.core.realtime.commitlog2.CommitLogAccessor;

/**
 * 从CommitLog中读取请求对象，并作用于UpdateHandler<br>
 * 有两个线程会访问这个对象:<br>
 * <li>
 * BuilderIndexThread  消费的线程
 * <li>
 * FullDumpThread      全量的线程
 * 
 * @author yusen
 *
 */
public class BuildIndexJob implements Runnable {
	protected static Log log = LogFactory.getLog(BuildIndexJob.class);

	private IndexSchema indexSchema;
	private PerfTracer perfTracer;
	
	volatile private UpdateHandler updateHandler = null;
	volatile private CommitLogAccessor commitLogAccessor;

	/* 标记是否需要暂停一下 */
	volatile boolean isPaused = false;

	public BuildIndexJob(CommitLogAccessor commitLog, UpdateHandler updateHandler, IndexSchema indexSchema) {
		this.updateHandler = updateHandler;
		this.commitLogAccessor = commitLog;
		this.indexSchema = indexSchema;
		perfTracer = new PerfTracer("BuilIndexJob-From-CommitLog", log) {
			protected void onTime() {
				log.warn(this.exportLog());
				log.warn(getStatistics());
				this.reset();
			}
		};
	}
	
	public void pause() {
		this.isPaused = true;
		log.warn("索引构建的任务暂停了...");
	}
	
	public void resume() {
		this.isPaused = false;
		log.warn("索引构建的任务恢复了...");
	}
	
	/* 访问计数器 */
	private AtomicInteger deleCounter = new AtomicInteger(0);
	private AtomicInteger addCounter  = new AtomicInteger(0);
	private AtomicInteger updateCounter = new AtomicInteger(0);
	private AtomicInteger deleByQueryCounter = new AtomicInteger(0);
	private boolean isCommit = false;
	
	/**
	 * FullDumpThread调用，全量完毕之后清零，从头计数
	 */
	public void resetCounter() {
		deleCounter.set(0);
		addCounter.set(0);
		updateCounter.set(0);
		deleByQueryCounter.set(0);
	}
	
	@Override
	public void run() {
		Object obj = null;
		int cout = 0;
		while (true) {
			String s = null;
			try {
				if(!isPaused) {
					obj = this.commitLogAccessor.read();
					if (obj != null) {
						final UpdateHandler updateHandler = this.getUpdateHandler();
						perfTracer.increment();
						if (obj instanceof AddDocumentRequest && !(obj instanceof UpdateDocumentRequest)) {
							AddDocumentRequest addReq = (AddDocumentRequest) obj;
							AddUpdateCommand addCmd = new AddUpdateCommand();
							addCmd.allowDups = false; //防止数据恢复的时候产生重复数据，故不管是Add还是Update一律按照Update来处理
							addCmd.solrDoc = addReq.solrDoc;
							s = addReq.getSolrDoc().toString();
							addCmd.doc = SolrCommandBuilder.getLuceneDocument(addReq.solrDoc, this.indexSchema);
							updateHandler.addDoc(addCmd);
							addCounter.incrementAndGet();
							
						} else if(obj instanceof UpdateDocumentRequest){
							AddDocumentRequest addReq = (AddDocumentRequest) obj;
							AddUpdateCommand addCmd = new AddUpdateCommand();
							addCmd.allowDups = false;
							addCmd.solrDoc = addReq.solrDoc;
							s = addReq.getSolrDoc().toString();
							addCmd.doc = SolrCommandBuilder.getLuceneDocument(addReq.solrDoc, this.indexSchema);
							updateHandler.addDoc(addCmd);
							updateCounter.incrementAndGet();
							
						} else if (obj instanceof DeleteByQueryRequest) {
							DeleteByQueryRequest cmd = (DeleteByQueryRequest) obj;
							DeleteUpdateCommand delCmd = new DeleteUpdateCommand();
							delCmd.query = cmd.query;
							updateHandler.delete(delCmd);
							deleByQueryCounter.incrementAndGet();
							
						} else if (obj instanceof DeleteByIdRequest) {
							DeleteByIdRequest cmd = (DeleteByIdRequest) obj;
							DeleteUpdateCommand delCmd = new DeleteUpdateCommand();
							delCmd.id = cmd.id;
							updateHandler.delete(delCmd);
							deleCounter.incrementAndGet();
						}
						
						isCommit = false;
						if(log.isDebugEnabled()) {
							log.debug("UpdateHanlder handled Number ==>" + cout++);
						}
					} else {
						
						//避免操作太不频繁，导致Commit一直不到阀值条件，从而影响实时性
						if(updateHandler instanceof TerminatorUpdateHandler && !isCommit) {
							UpdateHandler handler = ((TerminatorUpdateHandler)updateHandler).getProperUpdateHandler();
							if(handler instanceof RealTimeUpdateHandler) {
								isCommit = ((RealTimeUpdateHandler)handler).maybeCommit();
							}
						}
						Thread.sleep(100);
					}
				} else {
					log.error("BuildIndexJob paused,sleeping..");
					Thread.sleep(1000);
				}
			} catch (Throwable e) {
				log.error("Consume-Data-Error! Data ==> " + s, e);
			}
		}
	}

	public UpdateHandler getUpdateHandler() {
		return this.updateHandler;
	}

	public void setUpdateHandler(UpdateHandler updateHandler) {
		this.updateHandler = updateHandler;
	}

	public CommitLogAccessor getCommitLogAccessor() {
		return commitLogAccessor;
	}

	public void setCommitLogAccessor(CommitLogAccessor commitLog) {
		this.commitLogAccessor = commitLog;
	}
	
	public String getStatistics() {
		StringBuilder sb = new StringBuilder();
		sb.append("add: {").append(addCounter.get()).append("}").append("  ");
		sb.append("update: {").append(updateCounter.get()).append("}").append("  ");
		sb.append("deleteById: {").append(deleCounter.get()).append("}").append("  ");
		sb.append("deleteByQuery: {").append(deleByQueryCounter.get()).append("}").append("  ");
		
		return sb.toString();
	}
}
