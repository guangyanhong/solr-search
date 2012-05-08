package com.taobao.terminator.common.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

public class TerminatorZkClient implements Watcher {
	private static Log log = LogFactory.getLog(TerminatorZkClient.class);
	private static int zkTime = 3000;
	private OnReconnect onReconnect = new OnReconnect() {
		public void onReconnect(TerminatorZkClient zkClient) {
			
		}
	};

	private KeeperState    zkState;
	protected ZooKeeper    zookeeper;
	private String         zkAddress;
	private int            zkClientTimeout;
	private CountDownLatch connectedSignal ;
	private ScheduledExecutorService executor;

	public TerminatorZkClient(String zkAddress, int zkClientTimeout,OnReconnect onReconnect,boolean watchState) throws  TerminatorZKException {
		this.zkAddress       = zkAddress;
		this.zkClientTimeout = zkClientTimeout;
		this.zkState         = KeeperState.Disconnected;
		this.connectedSignal = new CountDownLatch(1);
		
		this.registOnReconnect(onReconnect);
		this.connect();
		
		if(watchState){
			ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
			scheduler.scheduleAtFixedRate(new ZkStateListener(this), 3000, 3000, TimeUnit.MILLISECONDS);
		}
	}
	
	public TerminatorZkClient(String zkAddress, int zkClientTimeout) throws  TerminatorZKException {
		this(zkAddress, zkClientTimeout, null,false);
	}
	
	public TerminatorZkClient(String zkAddress) throws  TerminatorZKException {
		this(zkAddress, zkTime, null,false);
	}
	
	private class ZkStateListener implements Runnable{
		private TerminatorZkClient zkClient;
		private static final int trialTime = 3;
		
		private ZkStateListener(TerminatorZkClient zkClient){
			this.zkClient = zkClient;
		}
		
		@Override
		public void run() {
			try{
				States state = zkClient.getZookeeper().getState();
				if(!state.isAlive() && state != States.CONNECTING && state != States.ASSOCIATING){
					log.warn("���Ӽ�������ֵ�ǰZooKeeper����״̬Ϊ������״̬,�ͻ��˿��ܶϿ��˺�ZkServer������,����Ҫ���½������ӣ����Դ���Ϊ [" + trialTime +"]");
					zkClient.reset();
					if(recreateConnect(trialTime)){
						log.warn("���½����������,����OnReconnect�¼�.");
						zkClient.onReconnect.onReconnect(zkClient);
					}else{
						log.fatal("["  + trialTime +"] �γ������½�������ʧ��");
					}
				}
			}catch(Throwable e){
				log.error("ZkStateListener Exception.",e);
			}
		}
		
		private boolean  recreateConnect(int trialTimes){
			if(trialTimes <= 0){
				return false;
			}
			try {
				zkClient.connect();
				return true;
			} catch (TerminatorZKException e) {
				log.warn("�����ؽ�����ʧ��[" + trialTimes + "].",e);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					log.error(e1,e1);
				}
				this.recreateConnect(trialTimes-1);
			}
			return false;
		}
	}
	
	protected void connect() throws  TerminatorZKException{
		try {
			zookeeper = new ZooKeeper(zkAddress, zkClientTimeout, this);
		} catch (IOException e) {
			throw new TerminatorZKException("Can't create ZooKeeper instance ,because of IOException",e);
		}

		try {
			boolean canConnectInTime = connectedSignal.await(zkClientTimeout,TimeUnit.MILLISECONDS);
			if(!canConnectInTime){
				throw new TerminatorZKException("Can't get connection from zkServer {" + zkAddress +"} in " + zkTime + " ms");
				
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new TerminatorZKException(e);
		}
	}
	
	protected void reset(){
		connectedSignal = new CountDownLatch(1);
	}

	@Override
	public synchronized void process(WatchedEvent event) {
		log.warn("TerminatorZkClient��watcher������.");
		
		zkState = event.getState();
		if (zkState == KeeperState.SyncConnected) {
			
			log.warn("ZooKeeper�ͻ��˳ɹ�������Server������.");
			connectedSignal.countDown();
			zookeeper.register(this);
			
		} else if (zkState == KeeperState.Expired) {//SessionʧЧ��ZooKeeper������,��Ҫ����newһ��ZooKeeper����
			/* 
			 * ==> If for some reason, the client fails to send heart beats to
			 * the server for a prolonged period of time (exceeding the
			 * sessionTimeout value, for instance), the server will expire the
			 * session, and the session ID will become invalid. The client
			 * object will no longer be usable. To make ZooKeeper API calls, the
			 * application must create a new client object. 
			 * 
			 * ==> If the ZooKeeper server the client currently connects to fails or otherwise does
			 * not respond, the client will automatically try to connect to
			 * another server before its session ID expires. If successful, the
			 * application can continue to use the client.
			 */
			log.warn("ZooKeeper�ͻ�����Server��SessionʧЧ,�������½�������.");
			executor = Executors.newScheduledThreadPool(1);
			executor.schedule(new Runnable() {
				private int delay = 1000;
				private int totalTimes = 0;
				public void run() {
					reset();
					boolean suc = false;
					totalTimes++;
					try {
						connect();
						suc = true;
					} catch (TerminatorZKException e) {
						log.warn("���½�����ZKServer������ʧ�� ==> " + totalTimes,e);
					}
					
					if(suc){
						if (onReconnect != null){
							try {
								onReconnect.onReconnect(TerminatorZkClient.this);
							} catch (Exception e) {
								log.error("����OnReconnectʱ�׳��쳣",e);
							}
						}
//						executor.shutdownNow();
					}else{
						if(delay < 240000) {
				            delay = delay * 2;
				          }
				          executor.schedule(this, delay, TimeUnit.MILLISECONDS);
					}
				}
			}, 1000, TimeUnit.MILLISECONDS);
			
		}else if(zkState == KeeperState.Disconnected){
			log.fatal("��ZooKeeper-Server�����ӶϿ���.Zookeeper���Զ��ؽ�����.");
		}else{
			log.warn("��ǰZooKeeper��״̬ ==> " + zkState);
		}
	}
	
	/**
	 * ����Znode,������Znode��ֵ�������Znode�Ѿ����ڣ���ֻ����znode��ֵ
	 * @param path
	 * @param bytes
	 * @param isPersistent
	 * @throws TerminatorZKException
	 */
	public void create(String path,byte[] bytes,boolean isPersistent) throws TerminatorZKException{
		path = TerminatorZKUtils.normalizePath(path);
		CreateMode createMode = isPersistent?CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
		try {
			zookeeper.create(path, bytes, Ids.OPEN_ACL_UNSAFE, createMode);
		} catch (KeeperException e) {
			if(e instanceof KeeperException.NoNodeException){
				throw new TerminatorZKException("The node ["  + e.getPath() + "]'s parent node doesn't exist,can't create it. ", e);
			}else if(e instanceof KeeperException.NodeExistsException){
				this.setData(path, bytes);
			}else{
				throw new TerminatorZKException("Other error",e);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new TerminatorZKException(e);
		} catch(Throwable e){
			throw new TerminatorZKException(e);
		}
	}
	
	public boolean createPathIfAbsent(String path, byte[] bytes, boolean isPersistent) {
		CreateMode createMode = isPersistent ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
		path = TerminatorZKUtils.normalizePath(path);
		try {
			zookeeper.create(path, bytes, Ids.OPEN_ACL_UNSAFE, createMode);
		} catch (KeeperException e) {
			return false;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		} catch(Throwable e){
			return false;
		}
		return true;
	}
	
	public boolean useAble() {
		return zookeeper != null && zookeeper.getState() == ZooKeeper.States.CONNECTED;
	}
	
	/**
	 * ����Znode(�־û�Znode)������Znode��ֵ
	 * 
	 * @param path
	 * @param bytes
	 * @throws TerminatorZKException
	 */
	public void create(String path,byte[] bytes) throws TerminatorZKException{
		this.create(path, bytes, true);
	}
	/**
	 * �ݹ鴴��znode������Znode��ֵ
	 * 
	 * @param path
	 * @param bytes
	 * @throws TerminatorZKException
	 */
	public void rcreate(String path,byte[] bytes)throws TerminatorZKException{
		this.rcreatePath(path);
		this.setData(path, bytes);
	}
	
	/**
	 * �ݹ鴴��Znode·��������ֵ
	 * @param path
	 * @return
	 * @throws TerminatorZKException
	 */
	public String rcreatePath(String path) throws TerminatorZKException {
		path = TerminatorZKUtils.normalizePath(path);
		if(this.exists(path)) 
			return path;
		String[] splits = path.substring(1).split(TerminatorZKUtils.SEPARATOR);
		String _p = "";
		for (String split : splits) {
			_p = _p + TerminatorZKUtils.SEPARATOR + split;
			if (!this.exists(_p)) {
				this.createPath(_p);
			}
		}
		return path;
	}
	
	/**
	 * �����־û���Znode��Path,������ڣ��򷵻�
	 * @param path
	 * @throws TerminatorZKException
	 */
	public void createPath(String path) throws TerminatorZKException{
		this.createPath(path, true);
	}
	
	/**
	 * ����Znode��Path��������ڣ��򷵻�
	 * @param path
	 * @param isPersistent
	 * @throws TerminatorZKException
	 */
	public void createPath(String path,boolean isPersistent)throws TerminatorZKException{
		CreateMode createMode = isPersistent?CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
		path = TerminatorZKUtils.normalizePath(path);
		try {
			if(!this.exists(path)){
				zookeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, createMode);
			}
		} catch (KeeperException e) {
			if(e instanceof KeeperException.NodeExistsException){
				//do nothing
			}else if(e instanceof KeeperException.NoNodeException){
				throw new TerminatorZKException("The node ["  + e.getPath() + "]'s parent node doesn't exist,can't create it. ", e);
			}else{
				throw new TerminatorZKException("Other error",e);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new TerminatorZKException(e);
		} catch(Throwable e){
			throw new TerminatorZKException(e);
		}
	}
	
	/**
	 * ����Path�����û�и�node,����Ѿ������򷵻�false
	 * @param path
	 * @param isPersistent
	 * @throws TerminatorZKException
	 */
	public boolean createPathIfAbsent(String path,boolean isPersistent){
		CreateMode createMode = isPersistent?CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
		path = TerminatorZKUtils.normalizePath(path);
		try {
			zookeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, createMode);
		} catch (KeeperException e) {
			return false;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		} catch(Throwable e){
			return false;
		}
		return true;
	}
	
	/**
	 * �жϸ�Znode�ڵ��Ƿ����
	 * @param path
	 * @return
	 * @throws TerminatorZKException
	 */
	public boolean exists(String path)throws TerminatorZKException{
		return this.exists(path, (Watcher)null);
	}
	
	/**
	 * �жϸ�Znode�ڵ��Ƿ����
	 * @param path
	 * @param watcher
	 * @return
	 * @throws TerminatorZKException
	 */
	public boolean exists(String path,Watcher watcher)throws TerminatorZKException{
		path = TerminatorZKUtils.normalizePath(path);
		
		if(watcher instanceof TerminatorWatcher){
			((TerminatorWatcher)watcher).setWatchType(TerminatorWatcher.WatcherType.EXIST_TYPE);
			((TerminatorWatcher)watcher).setZkClient(this);
		}
		
		try {
			Stat stat = zookeeper.exists(path, watcher);
			return stat!=null;
		} catch (KeeperException e) {
			throw new TerminatorZKException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new TerminatorZKException(e);
		}catch(Throwable e){
			throw new TerminatorZKException(e);
		}
	}
	
	/**
	 * ɾ��Znode�ڵ�
	 * @param path
	 * @return
	 * @throws TerminatorZKException
	 */
	public boolean delete(String path)throws TerminatorZKException{
		path = TerminatorZKUtils.normalizePath(path);
		try {
			zookeeper.delete(path, -1);
			return true;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new TerminatorZKException("UnKnown", e);
		} catch (KeeperException e) {
			if(e instanceof KeeperException.NoNodeException){
				throw new TerminatorZKException("Node does not exist,path is [" + e.getPath() + "].",e);
			}else if(e instanceof KeeperException.NotEmptyException){
				throw new TerminatorZKException("The node has children,can't delete it.",e);
			}else{
				throw new TerminatorZKException("UnKnown.",e);
			}
		} catch(Throwable e){
			throw new TerminatorZKException(e);
		}
	}
	
	/**
	 * �ݹ�ɾ����Znode�ڵ�
	 * @param path
	 * @throws TerminatorZKException
	 */
	public void rdelete(String path)throws TerminatorZKException{
		path = TerminatorZKUtils.normalizePath(path);
		
		try {
			zookeeper.delete(TerminatorZKUtils.normalizePath(path), -1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new TerminatorZKException(e);
		} catch (KeeperException e) {
			if(e instanceof KeeperException.NotEmptyException){
					List<String> children = null;
					try {
						children = zookeeper.getChildren(path, false);
					} catch (KeeperException e1) {
						if(e1 instanceof KeeperException.NoNodeException){
							throw new TerminatorZKException("Node does not exist,path is [" + e.getPath() + "].",e);
						}
					} catch (InterruptedException e1) {
						throw new TerminatorZKException(e);
					}
					for(String child : children){
						String _path = path + TerminatorZKUtils.SEPARATOR + child;
						this.rdelete(_path);
					}
					this.rdelete(path);
			}else if(e instanceof KeeperException.NoNodeException){
				throw new TerminatorZKException("Node does not exist,path is [" + e.getPath() + "].",e);
			}
		}
	}
	
	public byte[] getData(String path)throws TerminatorZKException{
		return this.getData(path, (Watcher)null);
	}
	
	/**
	 * ��ȡ��Ӧznode�ڵ������
	 * 
	 * @param path
	 * @param watcher
	 * @return
	 * @throws TerminatorZKException
	 */
	public byte[] getData(String path,Watcher watcher)throws TerminatorZKException{
		path = TerminatorZKUtils.normalizePath(path);
		if(watcher instanceof TerminatorWatcher){
			((TerminatorWatcher)watcher).setWatchType(TerminatorWatcher.WatcherType.GETDATA_TYPE);
			((TerminatorWatcher)watcher).setZkClient(this);
		}
		
		byte[] data = null;
		try {
			data = zookeeper.getData(path, watcher, null);
		} catch (KeeperException e) {
			if(e instanceof KeeperException.NoNodeException){
				throw new TerminatorZKException("Node does not exist,path is [" + e.getPath() + "].",e);
			}else{
				throw new TerminatorZKException(e);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new TerminatorZKException(e);
		}
		return data;
	}
	
	/**
	 * ��ȡ��Ӧznode�ڵ�ĺ��ӽڵ㣬�����Ļ�ȡ���ӽڵ㣬�����κε�Watch
	 * 
	 * @param path
	 * @return
	 * @throws TerminatorZKException
	 */
	public List<String> getChildren(String path)throws TerminatorZKException{
		return this.getChildren(path, (Watcher)null);
	}
	
	/**
	 * ��ȡ��Ӧ��znode�ڵ�ĺ��ӽڵ㣬������Watcher���������ڼ����ýڵ�Ļ��ڽڵ�ı��
	 * 
	 * @param path
	 * @param watcher
	 * @return
	 * @throws TerminatorZKException
	 */
	public List<String> getChildren(String path,Watcher watcher)throws TerminatorZKException{
		path = TerminatorZKUtils.normalizePath(path);
		if(watcher instanceof TerminatorWatcher){
			((TerminatorWatcher)watcher).setWatchType(TerminatorWatcher.WatcherType.GETCHILDREN_TYPE);
			((TerminatorWatcher)watcher).setZkClient(this);
		}
		
		List<String> children = null;
			try {
				children = zookeeper.getChildren(path, watcher);
			} catch (KeeperException e) {
				if(e instanceof KeeperException.NoNodeException){
					throw new TerminatorZKException("Node does not exist,path is [" + e.getPath() + "].",e);
				}else{
					throw new TerminatorZKException(e);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new TerminatorZKException(e);
			}
		return children;
	}

	/**
	 * ��znode�ڵ㸳ֵ
	 * 
	 * @param path
	 * @param bytes
	 * @throws TerminatorZKException
	 */
	public void setData(String path,byte[] bytes)throws TerminatorZKException{
		this.setData(path, bytes, -1);
	}
	
	public void setData(String path,byte[] bytes,int version)throws TerminatorZKException{
		path = TerminatorZKUtils.normalizePath(path);
		try {
			zookeeper.setData(path, bytes, version);
		} catch (KeeperException e) {
			if(e instanceof KeeperException.NoNodeException){
				this.rcreate(path, bytes);
			}else if(e instanceof KeeperException.BadVersionException){
				throw new TerminatorZKException("Bad Version,path [" + e.getPath() +"] version [" + version+"],the given version does not match the node's version", e);
			}else{
				throw new TerminatorZKException("May be value(byte[]) is larger than 1MB.",e);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new TerminatorZKException(e);
		}
	}
	
	/**
	 * ���ļ�Ŀ¼����״�ṹ����path���µĺ��ӽṹ
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public String showFolder(String path) throws Exception{
		StringBuilder sb = new StringBuilder();
		this.showFolder(path, sb, 0);
		return sb.toString();
	}
	
	/**
	 * ��Ŀ¼�Ĳ�νṹ��ʾznode�Ĳ�νṹ
	 * 
	 * @param basePath
	 * @param sb
	 * @param n
	 * @throws Exception
	 */
	private void showFolder(String basePath,StringBuilder sb,int n) throws Exception{
		for (int i = 0; i < n; i++) {
			sb.append("   |");
		}
		sb.append("---");
		String name = null;
		if(basePath.equals("/") || basePath.equals("")){
			name = "[ZK-ROOT]";
			basePath = "";
		}else{
			name = basePath.substring(basePath.lastIndexOf("/")+1) + "";
			byte[] bs = zookeeper.getData(basePath, false, null);
			if(bs==null){
				name = name + " [empty] ";
			}
		}
		sb.append(name).append("\n");

		List<String> children = zookeeper.getChildren(TerminatorZKUtils.normalizePath(basePath), false);
		if(children != null && !children.isEmpty()){
			for(String child : children){
				this.showFolder(basePath + "/" + child, sb,n+1);
			}
		}
	}
	
	/**
	 * ע���������Ӻ���¼�������
	 * 
	 * @param onReconnect
	 */
	public void registOnReconnect(OnReconnect onReconnect){
		if(onReconnect != null){
			this.onReconnect = onReconnect;
		}
	}
	
	/**
	 * �жϵ�ǰZookeeper�����Ƿ����(�����Ƿ�ϵ�)
	 * @return
	 */
	public boolean isAlive(){
		return this.getState().isAlive();
	}
	
	public States getState(){
		return zookeeper.getState();
	}
	
	public void close(){
		try {
			zookeeper.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		zookeeper = null;
	}

	public OnReconnect getOnReconnect() {
		return onReconnect;
	}

	public void setOnReconnect(OnReconnect onReconnect) {
		this.onReconnect = onReconnect;
	}

	public KeeperState getZkState() {
		return zkState;
	}

	public void setZkState(KeeperState zkState) {
		this.zkState = zkState;
	}

	public ZooKeeper getZookeeper() {
		return zookeeper;
	}

	public void setZookeeper(ZooKeeper zookeeper) {
		this.zookeeper = zookeeper;
	}

	public String getZkAddress() {
		return zkAddress;
	}

	public void setZkAddress(String zkAddress) {
		this.zkAddress = zkAddress;
	}

	public int getZkClientTimeout() {
		return zkClientTimeout;
	}

	public void setZkClientTimeout(int zkClientTimeout) {
		this.zkClientTimeout = zkClientTimeout;
	}

	public CountDownLatch getConnectedSignal() {
		return connectedSignal;
	}

	public void setConnectedSignal(CountDownLatch connectedSignal) {
		this.connectedSignal = connectedSignal;
	}
}
