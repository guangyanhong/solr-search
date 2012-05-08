//package com.taobao.terminator.core.realtime.service;
//
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.CountDownLatch;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import com.taobao.config.client.Subscriber;
//import com.taobao.config.client.SubscriberDataObserver;
//import com.taobao.config.client.SubscriberRegistrar;
//import com.taobao.config.client.SubscriberRegistration;
//import com.taobao.terminator.common.utils.HSFUtils;
//import com.taobao.terminator.core.realtime.commitlog2.SegmentPoint;
//
///**
// * ��ȫ��Dump���֮����������ڵ�Follower������֪ͨ����������Index�ļ���ͬ����������<br>
// * ���ฺ���������Follower�����������һ��������Follower����ı���伴�ɸ��·����б�<br>
// *
// * @author yusen
// */
//public class FollowersAccessor implements FollowerService{
//	private final Log log = LogFactory.getLog(FollowersAccessor.class);
//
//	private Map<String, FollowerService> services;
//	private String coreName;
//
//	public FollowersAccessor(String coreName) {
//		this.coreName = coreName;
//		this.registerWatcher();
//	}
//
//	private void registerWatcher() {
////		/* �������ڵ�Follower�����ķ����� */
////		SubscriberRegistration registration = new SubscriberRegistration("Terminator-InnerService-Subscriber", FollowerService.Utils.genCSDataId(coreName));
////		registration.setGroup(FollowerService.Utils.DEFAULT_HSF_GROUP);
////		Subscriber subscriber = SubscriberRegistrar.register(registration);
////
////		services = new HashMap<String,FollowerService>();
////
////		subscriber.setDataObserver(new FollowerServiceListener());
//	}
//
//	private CountDownLatch latch = null;
//
//	public void registerLatch(CountDownLatch latch) {
//		this.latch = latch;
//	}
//
//	/**
//	 * ֪ͨ���ڵ����е�Follower����
//	 *
//	 * @return ֪ͨ�ɹ��ĸ���
//	 */
//	public int notifyFollower(String ip, int port, String[] fileNames,SegmentPoint fullPoint) {
//		int sucCount = 0 ;
//		if(!services.isEmpty()) {
//			Collection<FollowerService> sl = services.values();
//			for(FollowerService s : sl) {
//				try {
//					s.notifyFollower(ip, port, fileNames,fullPoint);
//					sucCount ++;
//				} catch (Exception e) {
//					log.error("Notify Follower ERROR,ignore it!!",e);
//					latch.countDown();
//				}
//			}
//		}
//		return sucCount;
//	}
//
//	public int getFollowerCount() {
//		synchronized (services) {
//			return services.size();
//		}
//	}
//
//	/**
//	 * ���ڵ�Follower��ɫ�ķ��������
//	 *
//	 * @author yusen
//	 */
//	public class FollowerServiceListener implements SubscriberDataObserver {
//		@Override
//		public void handleData(String dataId, List<Object> datas) {
//			synchronized (services) {
//				Set<String> newVersions = this.parse(datas);
//				for (String newVersion : newVersions) {
//					if (!services.containsKey(newVersion)) {
//						try {
//							FollowerService fs = (FollowerService) HSFUtils.subscribe(FollowerService.class.getName(), newVersion).getObject();
//							services.put(newVersion, fs);
//						} catch (Exception e) {
//							// TODO
//						}
//					}
//				}
//
//				Set<String> versions = services.keySet();
//				for (String version : versions) {
//					if (!newVersions.contains(version)) {
//						services.remove(version);
//					}
//				}
//
//				log.warn("FollowerServices ==> {" + services.keySet() + "}");
//			}
//		}
//
//		private Set<String> parse(List<Object> datas) {
//			Set<String> set = new HashSet<String>();
//			for (Object o : datas) {
//				set.add((String) o);
//			}
//			return set;
//		}
//	}
//}
