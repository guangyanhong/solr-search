package com.taobao.terminator.common.data;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ds1:2,3,5-7, 9 ;ds2: 1,4,11,20
 * Ĭ�ϵķֿ�ֱ���������
 * 
 * @author yusen
 *
 */
public class DefaultTableDescriptionParser implements TableDescriptionParser {
	
	protected Log logger = LogFactory.getLog(DefaultTableDescriptionParser.class);
	protected static DecimalFormat format = new DecimalFormat("_0000");
	
	protected String preprocess(String raw) {
		return raw.replaceAll(" ", "");
	}
	
	protected void generateZone(String begin, String end, List<String> store) throws SubtableDescParseException {
		try {
			int head = Integer.parseInt(begin);
			int tail = Integer.parseInt(end);
			for(int i=head;i<=tail;i++) {
				store.add(format.format(i));
			}
		} catch(NumberFormatException nfe) {
			logger.error("��������ʱ����ת������", nfe);
			throw new SubtableDescParseException("��������ʱ����ת������", nfe);
		}
	}

	@Override
	public Map<String, List<String>> parse(String raw) throws SubtableDescParseException {
		String base = this.preprocess(raw);
		Map<String, List<String>> store = new LinkedHashMap<String, List<String>>();
		StringBuilder sb = new StringBuilder();
		boolean hasZone = false;
		String dsName = null;
		String begin = null;
		String end = null;
		for(char c: base.toCharArray()) {
			switch(c) {
			case ':':
				dsName = sb.toString();
				store.put(dsName, new ArrayList<String>());
				sb = new StringBuilder();
				break;
			case '-':
				if(sb.length() == 0 || hasZone) {
					logger.error("����-ʱ���������䣬�����ڵ�ǰ���䴦�����ǰ���ܴ����µ�����");
					throw new SubtableDescParseException("����-ʱ���������䣬�����ڵ�ǰ���䴦�����ǰ���ܴ����µ�����");
				}
				begin = sb.toString();
				sb = new StringBuilder();
				hasZone = true;
				break;
			case ';':
			case ',':
				if(dsName==null) {
					//������$����ʱ�������������ֱ������
					break;
				}
				if(hasZone) {
					end = sb.toString();
					this.generateZone(begin, end, store.get(dsName));
					hasZone =false;
				} else {
					store.get(dsName).add(format.format(Long.parseLong(sb.toString())));
				}
				sb = new StringBuilder();
				break;
			case '$':
				sb = new StringBuilder();
				//$��ʾ�����������dsNameȥ������������
				if(store.get(dsName).size() != 0) {
					logger.error("����$���ţ���Ӧ���к�׺");
					throw new SubtableDescParseException("����$���ţ���Ӧ���к�׺");
				}
				dsName = null;
				break;
			default:
				sb.append(c);
			}
		}
		
		if(sb.length() != 0) {
			if(hasZone) {
				end = sb.toString();
				this.generateZone(begin, end, store.get(dsName));
			} else {
				store.get(dsName).add(sb.toString());
			}
			sb = new StringBuilder();
		}
		
		return store;
	}

	public static void main(String[] args) throws Exception {
		
		TableDescriptionParser parser = new DefaultTableDescriptionParser();
		String raw = "ds1:2,3,5-7, 9 ;ds2: 1,4,11,20, 21-25;ds3:$;ds4:30-32";
		long begin = System.nanoTime();
		System.out.println(parser.parse(raw));
		System.out.println("total spend time: " + (System.nanoTime()-begin)/1000000.0 + "ms");
	}
}
