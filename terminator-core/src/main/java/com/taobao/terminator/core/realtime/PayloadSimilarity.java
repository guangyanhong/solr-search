package com.taobao.terminator.core.realtime;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Similarity;
import org.apache.solr.common.params.SolrParams;

import com.taobao.terminator.core.realtime.DefaultSearchService.SortFieldThreadLocalContext;

/**
 * 为了提高Lucene的指定字段排序的性能，直接由排序字段的值影响查询结果的分值，通过Payload的方式获取需要排序的字段的值，并影响打分。且只有Payload影响打分。
 * 
 * @author yusen
 */
public class PayloadSimilarity extends Similarity {
	private static final long serialVersionUID = 1L;
	
	private static Map<String,Integer> length_maps = new HashMap<String,Integer>();
	static {
		length_maps.put("LONG" , 8);
		length_maps.put("INT"  , 4);
		length_maps.put("SHORT", 2);
	}
	
	private Map<String,OffsetAndLength> maps = null;
	
	public PayloadSimilarity(SolrParams params) {
		Iterator<String> nameItr = params.getParameterNamesIterator();
		
		List<Pair> pl = new ArrayList<Pair>();
		
		while(nameItr.hasNext()) {
			String name = nameItr.next();
			String value = params.get(name);
			if(!length_maps.containsKey(value)) {
				throw new RuntimeException("Undefined-Type in schema.xml /similarity/str/" + name);
			}
			
			pl.add(new Pair(name, value));
		}
		
		Collections.sort(pl, new Comparator<Pair>(){
			public int compare(Pair p1, Pair p2) {
				return p1.name.compareTo(p2.name);
			}
			
		});
		
		maps = new LinkedHashMap<String, OffsetAndLength>(); 
		
		int offset = 0;
		for(Pair p : pl) {
			maps.put(p.name.substring(2), new OffsetAndLength(offset,length_maps.get(p.value)));
			offset = offset + length_maps.get(p.value);
		}
	}

	public float scorePayload(String fieldName, byte[] payload, int offset, int length) {
		String sfName = SortFieldThreadLocalContext.getInstance().get();
		if(sfName == null || sfName.trim().equals("")) {
			return 1;
		}
		
		try {
			String[] ps = sfName.split(" ");
			String name = ps[0];
			String sortType = ps[1];
			
			boolean isDesc = true;
			if(sortType.equals("desc")) {
				isDesc = true;
			} else if(sortType.equals("asc")) {
				isDesc = false;
			} else {
				return 1;
			}
			
			int o = maps.get(name).offset;
			int l  = maps.get(name).length;
			byte[] b = new byte[l];
			System.arraycopy(payload, o, b, 0, l);
			
			if(b.length == 2) {
				short s =  (short)(((b[0] & 0xFF) << 8) | ((b[1] & 0xFF) << 0));
				return isDesc ? s : (float)(1.0/s);
			} else if(b.length == 4) { //int型转换成float型会有精度的损失，这个问题只能通过业务上进行限制，比如20101226这样的表述日期的数字，
									    //尽量省去‘20’前缀，使得整体数值降下来，如果7位的int的话，则应该没什么大问题的
				int i = ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16)  | ((b[2] & 0xFF) <<  8) |  (b[3] & 0xFF);
				return isDesc ? i : (float)((1.0 * 1000)/i);
			} else if(b.length == 8) {//FIXME 这个地方Long型转换成Float精度损失会非常大，目前业务上还没有这样的需求，所以暂时把这个问题放在这儿，日后解决
				long lo = ((long)b[0] << 56) +
				((long)(b[1] & 255) << 48) +
				((long)(b[2] & 255) << 40) +
				((long)(b[3] & 255) << 32) +
				((long)(b[4] & 255) << 24) +
				((b[5] & 255) << 16) +
				((b[6] & 255) <<  8) +
				((b[7] & 255) <<  0);
				return isDesc ? lo : (float)(1.0/lo);
			} else {
				return 1;
			}
		} catch (Throwable e) {
			return 1;
		}
	}
	
	public static void main(String[] args) throws Exception{
        //TODO
//		short s = 12345;
//		ByteArrayOutputStream dot = new ByteArrayOutputStream();
//		DataOutputStream dout = new DataOutputStream(dot);
//		dout.writeShort(s);
//		byte[] b = dot.toByteArray().toByteArray();
//		System.out.println(((b[0] & 0xFF) << 8) | ((b[1] & 0xFF) << 0));
		
		/*BigDecimal bd = new BigDecimal(Integer.MAX_VALUE);
		System.out.println(bd.floatValue());
		
		BigDecimal bd2 = new BigDecimal(Integer.MAX_VALUE-3);
		System.out.println(bd2.floatValue());
		
		System.out.println((float)20101231);
		System.out.println(Float.parseFloat(2010123200.0 + ""));
		
		System.out.println((float)20101232);
		System.out.println(Float.parseFloat(2010123100.0 + ""));
		
		System.out.println(2.01012314E9);*/
		
		/*System.out.println((float)121220);
		System.out.println((float)121221);
		System.out.println((float)121222);
		System.out.println((float)121223);*/
		
		/*System.out.println((float)(9000001));
		System.out.println((float)(9000002));
		System.out.println((float)(9000003));
		System.out.println((float)(9000004));
		
		System.out.println((float)(1.0/9110428));
		System.out.println((float)(1.0/9110429));
		System.out.println((float)(1.0/9110430));
		System.out.println((float)(1.0/9110431));
		
		System.out.println(Integer.MAX_VALUE);*/
		
	}

	public float scorePayload(int docId, String fieldName, int start, int end, byte[] payload, int offset, int length) {
		return this.scorePayload(fieldName, payload, offset, length);
	}

	@Override
	public float coord(int overlap, int maxOverlap) {
		return 1;
	}

	@Override
	public float idf(int docFreq, int numDocs) {
		return 1;
	}

	@Override
	public float lengthNorm(String fieldName, int numTokens) {
		return 1;
	}

	@Override
	public float queryNorm(float sumOfSquaredWeights) {
		return 1;
	}

	@Override
	public float sloppyFreq(int distance) {
		return 1;
	}

	@Override
	public float tf(float freq) {
		return 1;
	}
	
	private class Pair {
		public Pair(String name, String value) {
			super();
			this.name = name;
			this.value = value;
		}
		public String name;
		public String value;
	}
	
	private class OffsetAndLength {
		public int offset;
		public int length;
		public OffsetAndLength(int offset, int length) {
			super();
			this.offset = offset;
			this.length = length;
		}
	}
}
