package com.taobao.terminator.core.realtime.service;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;

public class SolrUtils {
	public static SolrCore createNewCore(CoreContainer coreContainer,String coreName) throws Exception {
		SolrCore oldCore = coreContainer.getCore(coreName);
		CoreDescriptor coreDesc = oldCore.getCoreDescriptor();
		String oldDataDirStr = coreDesc.getDataDir();

		File parentDir = new File(oldDataDirStr).getParentFile();
		File tmpDir = null;
		/**
		 * fww add
		 * oldDataDirStr�ļ���β���ڶ�����ʽ��1.data-back/ 2.data-back ��
		 * ԭ���ж�ֻ��data-back��β���жϲ���ȫ,�޸�Ϊ��������ʽcontains()��ʽ
		 */
		if (oldDataDirStr.contains("data-back")) {
			tmpDir = new File(parentDir, "data");
		} else {
			tmpDir = new File(parentDir, "data-back");
		}

		boolean newCreate = false;
		if (newCreate = !tmpDir.exists()) {
			tmpDir.mkdirs();
		}

		if (!newCreate) {
			FileUtils.cleanDirectory(tmpDir);
		}

		coreDesc.setDataDir(tmpDir.getAbsolutePath());
		return coreContainer.create(coreDesc);
	}
	
	public static String[] listIndexFile(File indexDir) {
		return indexDir.list();
	}
}
