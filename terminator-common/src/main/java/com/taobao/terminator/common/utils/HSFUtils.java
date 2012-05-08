//package com.taobao.terminator.common.utils;
//
//import com.taobao.config.client.Publisher;
//import com.taobao.config.client.PublisherRegistrar;
//import com.taobao.config.client.PublisherRegistration;
//import com.taobao.hsf.app.spring.util.HSFSpringConsumerBean;
//import com.taobao.hsf.app.spring.util.HSFSpringProviderBean;
//import com.taobao.terminator.common.TerminatorConstant;
//
//public class HSFUtils {
//
//	/**
//	 * ����HSF����
//	 *
//	 * @param ifName
//	 * @param version
//	 * @return
//	 * @throws Exception
//	 */
//	public static HSFSpringConsumerBean subscribe(String ifName,String version) throws Exception{
//		HSFSpringConsumerBean hsfConsumerBean = new HSFSpringConsumerBean();
//		hsfConsumerBean.setInterfaceName(ifName);
//		hsfConsumerBean.setVersion(version);
//		hsfConsumerBean.init();
//		return hsfConsumerBean;
//	}
//
//	/**
//	 * ����HSF����
//	 *
//	 * @param ifName
//	 * @param version
//	 * @param targetObj
//	 * @return
//	 */
//	public static HSFSpringProviderBean publish(String ifName,String version,Object targetObj) throws Exception{
//		HSFSpringProviderBean providerBean = new HSFSpringProviderBean();
//		providerBean.setTarget(targetObj);
//		providerBean.setServiceInterface(ifName);
//		providerBean.setServiceVersion(version);
//		providerBean.setSerializeType(TerminatorConstant.DEFAULT_SERIALIZE_TYPE);
//		providerBean.setClientTimeout(3000);
//		providerBean.init();
//		return providerBean;
//	}
//
//	/**
//	 * ����Config-Server��dataId->data���ݶ�
//	 *
//	 * @param dataId
//	 * @param data
//	 * @param groupId
//	 */
//	public static void publishCSData(String dataId,String data,String groupId) {
//		PublisherRegistration<String> registration = new PublisherRegistration<String>("Terminator-InnerService-Publisher",dataId );
//		registration.setGroup(groupId);
//		Publisher<String> publisher =  PublisherRegistrar.register(registration);
//		publisher.publish(data);
//	}
//}