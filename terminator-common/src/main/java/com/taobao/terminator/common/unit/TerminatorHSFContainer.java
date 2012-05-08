package com.taobao.terminator.common.unit;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.ServiceType;
import com.taobao.terminator.common.TerminatorConstant;
import com.taobao.terminator.common.TerminatorHsfPubException;
import com.taobao.terminator.common.TerminatorHsfSubException;
import com.taobao.terminator.common.protocol.MasterService;
import com.taobao.terminator.common.protocol.TerminatorService;

/**
 * 获取HSF服务容器类
 * 
 * @author yusen(lishuai)
 * @since 1.0, 2010-5-20 下午01:30:26
 */

public abstract class TerminatorHSFContainer {
	private static Log logger = LogFactory.getLog(TerminatorHSFContainer.class);

//	private static Map<String,HSFSpringConsumerBean> subscribedConsumerBeans = new HashMap<String,HSFSpringConsumerBean>();
//	private static Map<String,HSFSpringProviderBean> publishedProviderBeans = new HashMap<String,HSFSpringProviderBean>();
//
//	//className ==> instance object
//	private static Map<String,Object> publishedTargets = new HashMap<String,Object>();
//
//
//	public static HSFSpringConsumerBean getHSFSpringConsumerBean(ServiceType type,String coreName){
//		String verison = Utils.genearteVersion(type, coreName);
//		if(ServiceType.merger.equals(type) || ServiceType.reader.equals(type)){
//			return getSubscribedConsumerBean(TerminatorService.class.getName(), verison);
//		}else{
//			return getSubscribedConsumerBean(MasterService.class.getName(), verison);
//		}
//	}
//
//
//	public static void publishService(Object target, String interfaceName,String serviceVersion) throws TerminatorHsfPubException{
//		if (StringUtil.isBlank(serviceVersion)) {
//			throw new IllegalArgumentException("The argument [serviceVersion] can not be blank.");
//		}
//		String ifname = interfaceName != null ? interfaceName : TerminatorConstant.DEFAULT_INTERFACE_NAME;
//		String dataId = Utils.generateDataId(ifname, serviceVersion);
//		logger.warn("发布HSF服务 ==> " + dataId);
//
//		if(getPublishedProviderBean(dataId) == null){
//
//			HSFSpringProviderBean providerBean = new HSFSpringProviderBean();
//			providerBean.setTarget(target);
//			providerBean.setServiceInterface(ifname);
//			providerBean.setServiceVersion(serviceVersion);
//			providerBean.setSerializeType(TerminatorConstant.DEFAULT_SERIALIZE_TYPE);
//			try {
//				providerBean.init();
//			} catch (Exception e) {
//				throw new TerminatorHsfPubException("发布HSF服务异常,InterfaceName: {" + ifname +"} version: {" + serviceVersion +"},cause by:" ,e);
//			}
//
//			publishedProviderBeans.put(dataId, providerBean);
//		}
//
//		String targetCname = target.getClass().getName();
//		if(getPublishedTargetObject(targetCname) == null){
//			publishedTargets.put(targetCname, target);
//		}
//	}
//
//	/**
//	 * 订阅HSF服务
//	 *
//	 * @param interfaceName
//	 * @param serviceVersion
//	 * @throws TerminatorHsfSubException
//	 */
//	public static HSFSpringConsumerBean subscribeService(String interfaceName,String serviceVersion) throws TerminatorHsfSubException{
//		if (StringUtil.isBlank(serviceVersion)) {
//			throw new IllegalArgumentException("The argument [serviceVersion] can not be blank.");
//		}
//		String ifname = interfaceName != null ? interfaceName : TerminatorConstant.DEFAULT_INTERFACE_NAME;
//		String dataId = Utils.generateDataId(ifname, serviceVersion);
//
//		if(getSubscribedConsumerBean(dataId) == null){
//			HSFSpringConsumerBean hsfConsumerBean = new HSFSpringConsumerBean();
//
//			hsfConsumerBean.setInterfaceName(ifname);
//			hsfConsumerBean.setVersion(serviceVersion);
//			try {
//				hsfConsumerBean.init();
//			} catch (Exception e) {
//				throw new TerminatorHsfSubException("订阅HSF服务异常,InterfaceName: {" + ifname +"} version: {" + serviceVersion +"},cause by:" ,e);
//			}
//			subscribedConsumerBeans.put(dataId, hsfConsumerBean);
//			return hsfConsumerBean;
//		}
//		return null;
//	}
//
//	/**
//	 * 撤销已经订阅的服务
//	 *
//	 * @param interfaceName
//	 * @param serviceVersion
//	 */
//	public static void removeSubscribedService(String interfaceName,String serviceVersion){
//		if (StringUtil.isBlank(serviceVersion)) {
//			throw new IllegalArgumentException("The argument [serviceVersion] can not be blank.");
//		}
//		String ifname = interfaceName != null ? interfaceName : TerminatorConstant.DEFAULT_INTERFACE_NAME;
//		String dataId = Utils.generateDataId(ifname, serviceVersion);
//		if(getSubscribedConsumerBean(dataId) == null){
//			return;
//		}
//		subscribedConsumerBeans.remove(dataId);
//	}
//
//	/**
//	 * 获取已经订阅的HSF服务的HSFSpringCOnsumerBean对象
//	 *
//	 * @param dataId
//	 * @return
//	 */
//	public static HSFSpringConsumerBean getSubscribedConsumerBean(String dataId){
//		return subscribedConsumerBeans.get(dataId);
//	}
//
//	/**
//	 * 获取已经订阅的HSF服务的HSFSpringCOnsumerBean对象
//	 *
//	 * @param interfaceName
//	 * @param serviceVersion
//	 * @return
//	 */
//	public static HSFSpringConsumerBean getSubscribedConsumerBean(String interfaceName,String serviceVersion){
//		return  getSubscribedConsumerBean(Utils.generateDataId(interfaceName != null ? interfaceName : TerminatorConstant.DEFAULT_INTERFACE_NAME, serviceVersion));
//	}
//
//	/**
//	 * 获取已经发布的HSF服务的HSFSpringProviderBean对象
//	 *
//	 * @param dataId
//	 * @return
//	 */
//	public static HSFSpringProviderBean getPublishedProviderBean(String dataId){
//		return publishedProviderBeans.get(dataId);
//	}
//
//	/**
//	 * 获取已经发布的HSF服务的HSFSpringProviderBean对象
//	 *
//	 * @param interfaceName
//	 * @param serviceVersion
//	 * @return
//	 */
//	public static HSFSpringProviderBean getPublishedProviderBean(String interfaceName,String serviceVersion){
//		return getPublishedProviderBean(Utils.generateDataId(interfaceName != null ? interfaceName : TerminatorConstant.DEFAULT_INTERFACE_NAME, serviceVersion));
//	}
//
//	/**
//	 * 获取作为TargeObject发布为HSF服务的对象
//	 *
//	 * @param cname
//	 * @return
//	 */
//	public static Object getPublishedTargetObject(String cname){
//		return publishedTargets.get(cname);
//	}
//
//	public static TerminatorService getTerminatorService(String serviceVersion){
//		try {
//			return (TerminatorService)(getSubscribedConsumerBean((String)null,serviceVersion).getObject());
//		} catch (Exception e) {
//			return null;
//		}
//	}
//
//	public static MasterService getMasterService(String coreName){
//		try {
//			return (MasterService)(getHSFSpringConsumerBean(ServiceType.writer,coreName).getObject());
//		} catch (Exception e) {
//			return null;
//		}
//	}
//
//	/**
//	 * 提供Reade Merge服务
//	 * @param type
//	 * @param coreName
//	 * @return
//	 */
//	public static TerminatorService getTerminatorService(ServiceType type,String coreName){
//		try {
//			return (TerminatorService)(getHSFSpringConsumerBean(type,coreName).getObject());
//		} catch (Exception e) {
//			return null;
//		}
//	}
	
	/**
	 * HSF的工具类
	 * 
	 * @author yusen
	 */
	public static class Utils{

		public static String genearteVersion(ServiceType type, String coreName) {
			if (type.equals(ServiceType.merger)) {
				String[] s = coreName.split(TerminatorConstant.HSF_VERSION_SEPERATOR);
				String serviceName = s[0];
				return serviceName + TerminatorConstant.HSF_VERSION_SEPERATOR + type.getType();
			} else{
				return coreName + TerminatorConstant.HSF_VERSION_SEPERATOR + type.getType();
			}
		}
		
		public static String generateDataId(String interfaceName,String serviceVersion){
			return interfaceName + ":" + serviceVersion;
		}

		public static Set<String> generateTerminatorVersions(Set<String> coreNameSet) {
			Set<String> versionSet = new HashSet<String>();
			for (String coreName : coreNameSet) {
				versionSet.add(genearteVersion(ServiceType.reader, coreName));
				versionSet.add(genearteVersion(ServiceType.merger, coreName));
			}
			return versionSet;
		}
		
		public static Set<String> generateMasterVersions(Set<String> coreNameSet) {
			Set<String> versionSet = new HashSet<String>();
			for (String coreName : coreNameSet) {
				versionSet.add(genearteVersion(ServiceType.writer, coreName));
			}
			return versionSet;
		}
		
		public static Set<String> generateReadVersions(Set<String> coreNameSet){
			Set<String> versionSet = new HashSet<String>();
			for (String coreName : coreNameSet) {
				versionSet.add(genearteVersion(ServiceType.reader, coreName));
			}
			return versionSet;
		}
		
		public static Set<String> generateWriteVersions(Set<String> coreNameSet){
			Set<String> versionSet = new HashSet<String>();
			for (String coreName : coreNameSet) {
				versionSet.add(genearteVersion(ServiceType.writer, coreName));
			}
			return versionSet;
		}
		
		public static String generateMergeVersion(String coreName){
			return genearteVersion(ServiceType.merger, coreName);
		}
		
		public static String generateSlaveWriteService(String coreName,String ip){
			return genearteVersion(ServiceType.writer, coreName) +  TerminatorConstant.HSF_VERSION_SEPERATOR + ip;
		}
	}
}
