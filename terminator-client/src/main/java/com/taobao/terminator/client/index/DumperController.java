package com.taobao.terminator.client.index;

import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.zk.TerminatorZKException;
import com.taobao.terminator.common.zk.TerminatorZKUtils;
import com.taobao.terminator.common.zk.TerminatorZkClient;

public class DumperController {
	protected String basePath = null;
	
	private static Map<String,DumperController> dcs = null;

	private boolean            allowAllFullDump = false;
	private TerminatorZkClient zkClient         = null;
	private String             serviceName      = null;
	
	public static void createInstance(boolean allowAllFullDump,TerminatorZkClient zkClient,String serviceName){
		if(dcs == null){
			dcs = new HashMap<String,DumperController>();
		}
		if(!dcs.containsKey(serviceName)){
			dcs.put(serviceName, new DumperController(allowAllFullDump,zkClient,serviceName));
		}
	}
	
	public static DumperController getInstance(String serviceName){
		if(dcs == null || !dcs.containsKey(serviceName)){
			throw new NullPointerException("DumperController还未实例化，请确保是先调用了createInstance方法");
		}else{
			return dcs.get(serviceName);
		}
	}
	
	private DumperController(boolean allowAllFullDump,TerminatorZkClient zkClient,String serviceName) {
		this.allowAllFullDump = allowAllFullDump;
		this.zkClient = zkClient;
		this.serviceName = serviceName;
		// terminator/dump-controller  
		this.basePath = TerminatorZKUtils.contactZnodePaths(TerminatorZKUtils.TERMINATOR_ROOT_PATH, TerminatorZKUtils.DUMPER_CONTROLLER);
		
		try {
			zkClient.rcreatePath(basePath);
		} catch (TerminatorZKException e) {
			throw new RuntimeException("创建" + TerminatorZKUtils.DUMPER_CONTROLLER + "节点失败.",e);
		}
	}

	public boolean localhostCanRun(boolean isFull){
		if(allowAllFullDump && isFull){
			return true;
		}else{
			try {
				// terminator/dump-controller/search4tag
				String path = TerminatorZKUtils.contactZnodePaths(this.basePath, this.serviceName);
				String localIp = TerminatorCommonUtils.getLocalHostIP();
				boolean exists = zkClient.exists(path);
				if(!exists){
					boolean createSuc = false;
					try {
						zkClient.getZookeeper().create(path, localIp.getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
						createSuc = true;
					} catch (KeeperException e) {
						
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					return createSuc;
				}else{
					byte[] bytes = zkClient.getData(path);
					String ip = TerminatorZKUtils.toString(bytes);
					if(ip.equals(localIp)){
						return true;
					}else{
						return false;
					}
				}
			} catch (Exception e) {
				return false;
			}
		}
	}
	
	private String genPath(){
		return TerminatorZKUtils.contactZnodePaths(basePath, TerminatorCommonUtils.getLocalHostIP());
	}
}
