/**
 *  Rights reserved by www.taobao.com
 */
package com.taobao.terminator.core.solrx;

import org.apache.lucene.search.DefaultSimilarity;

/**
 * �����˾������ƥ�������
 * @author <a href="mailto:xiangfei@taobao.com"> xiangfei</a>
 * @version 2010-3-18:����11:55:25
 * 
 */
public class CompanySimilarity extends DefaultSimilarity {
	private static final long serialVersionUID = -3600864523501653144L;

	@Override
	public float tf(float freq) {
		return 1.0f;
	}

}
