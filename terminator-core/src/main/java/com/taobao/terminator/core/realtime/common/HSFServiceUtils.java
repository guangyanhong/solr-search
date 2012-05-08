//package com.taobao.terminator.core.realtime.common;
//
//import com.taobao.config.client.Publisher;
//import com.taobao.config.client.PublisherRegistrar;
//import com.taobao.config.client.PublisherRegistration;
//
//public class HSFServiceUtils {
//
//	/**
//	 * »ñÈ¡RealTimeServiceµÄVerison
//	 *
//	 * @param coreName
//	 * @return
//	 */
//	public static String genRTVersion(String coreName) {
//		return new StringBuilder().append("coreName").append("-").append("RT").toString();
//	}
//
//	public static String genCSDataId(String ifname,String version) {
//		return new StringBuilder().append(ifname).append(":").append(version).toString();
//	}
//
//	public static void publishCSData(String dataId,String data,String groupId) {
//		PublisherRegistration<String> registration = new PublisherRegistration<String>("Terminator-InnerService-Publisher",dataId );
//		registration.setGroup(groupId);
//		Publisher<String> publisher =  PublisherRegistrar.register(registration);
//		publisher.publish(data);
//	}
//}
