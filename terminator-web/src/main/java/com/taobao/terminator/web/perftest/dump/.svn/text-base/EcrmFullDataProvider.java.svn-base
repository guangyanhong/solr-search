package com.taobao.terminator.web.perftest.dump;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.data.DataProvider;

/**
 * 按照 卖家-> 买家数量分布比例生成数据
 * 
 * @author yusen
 *
 */
public class EcrmFullDataProvider implements DataProvider{
	private final Log log = LogFactory.getLog(EcrmFullDataProvider.class);
	
	public static final Info ONE_TO_ONEK = new Info(55,1,1000,1);
	public static final Info ONEK_TO_ONEW = new Info(35,1000,10000,2);
	public static final Info ONEW_TO_TENW = new Info(10,10000,100000,3);
	
	public static final Map<Integer,Info> allInfos = new HashMap<Integer,Info>();
	
	public static final int TOTAL_RATIO = ONE_TO_ONEK.ratio + ONEK_TO_ONEW.ratio + ONEW_TO_TENW.ratio;

	private int maxSellerId = 5000; 
	private int maxBuyerId  = 100000000;

	static {
		allInfos.put(ONE_TO_ONEK.type, ONE_TO_ONEK);
		allInfos.put(ONEK_TO_ONEW.type, ONEK_TO_ONEW);
		allInfos.put(ONEW_TO_TENW.type, ONEW_TO_TENW);
	}
	
	private int count = 0;
	private int maxCount = 2000;
	private int[] pool = new int[TOTAL_RATIO];
	
	private Iterator<Map<String, String>>  itr = null;
	
	@Override
	public void init() throws Exception {
		int i = 0;
		for (i = 0; i < ONE_TO_ONEK.ratio; i++) {
			pool[i] = ONE_TO_ONEK.type;
		}
		
		int j = i + ONEK_TO_ONEW.ratio;
		
		for (; i < j; i++) {
			pool[i] = ONEK_TO_ONEW.type;
		}

		j = i + ONEW_TO_TENW.ratio;
		for (; i < j; i++) {
			pool[i] = ONEW_TO_TENW.type;
		}
		
		int max1 = (int)((ONE_TO_ONEK.ratio/(TOTAL_RATIO*1.0)) * maxSellerId);
		int max2 = (int)(((ONE_TO_ONEK.ratio + ONEK_TO_ONEW.ratio)/(TOTAL_RATIO * 1.0)) * maxSellerId);
		int max3 = maxSellerId;
		
		int[] sellerIds1 = new int[max1];
		for(int num=0;num<max1;num++) {
			sellerIds1[num] = num;
		}
		ONE_TO_ONEK.sellerIds = sellerIds1;
		
		int[] sellerIds2 = new int[max2-max1];
		for(int num = 0;num<max2-max1;num++) {
			sellerIds2[num] = num  + max1;
		}
		ONEK_TO_ONEW.sellerIds = sellerIds2;
		
		int[] sellerIds3 = new int[max3-max2];
		for(int num = 0;num<max3-max2;num++) {
			sellerIds3[num] = num + max2;
		}
		ONEW_TO_TENW.sellerIds = sellerIds3;
	}

	
	@Override
	public boolean hasNext() throws Exception {
		if(count++%10000 == 0) {
			log.warn("当前Dump的总数 ==> " + count);
		}
		return count <= maxCount;
	}
	
	@Override
	public Map<String, String> next() throws Exception {
		if(itr == null || !itr.hasNext()) {
			Random random = new Random();
			random.setSeed(random.nextLong() ^ System.currentTimeMillis() ^ hashCode() ^ Runtime.getRuntime().freeMemory());
			
			int type = pool[random.nextInt(TOTAL_RATIO)];
			Info info = allInfos.get(type);
			
			//长生minNum ~ maxNum个数据
			itr = this.genDatas(info).iterator();
			return itr.next();
		} else {
			return itr.next();
		}
	}
	
	private List<Map<String,String>> genDatas(Info info) {
		int min = info.minNum;
		int max = info.maxNum;
		
		int midNum = (min + max) / 2;
		int range = (max - min) / 2;
		
		Random random = new Random();
		random.setSeed(random.nextLong() ^ System.currentTimeMillis() ^ hashCode() ^ Runtime.getRuntime().freeMemory());
		int num = random.nextInt(range);
		int targetNum = 0;
		if(random.nextBoolean()) {
			targetNum = midNum + num;
		} else {
			targetNum = midNum - num;
		}
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();
		int sellerId = info.randomSellerId();
		for(int i=0;i<targetNum;i++) {
			Map<String,String> data = this.genData(sellerId, random.nextInt(maxBuyerId));
			list.add(data);
		}
		
		return list;
	}
	
	@Override
	public void close() throws Exception {
		itr = null;
		count = 0;
		uniqueId = 0;
	}
	
	public static final List<String> cityEnums = new ArrayList<String>();
	public static int cityNum = 0;
	static {
		cityEnums.add("杭州市1");
		cityEnums.add("杭州市2");
		cityEnums.add("杭州市3");
		cityEnums.add("杭州市4");
		cityEnums.add("北京市1");
		cityEnums.add("北京市2");
		cityEnums.add("北京市3");
		cityEnums.add("北京市4");
		cityEnums.add("上海市1");
		cityEnums.add("上海市2");
		cityEnums.add("上海市3");
		cityEnums.add("上海市4");
		cityEnums.add("成都市1");
		cityEnums.add("成都市2");
		cityEnums.add("成都市3");
		cityEnums.add("成都市4");
		cityEnums.add("武汉市1");
		cityEnums.add("武汉市2");
		cityEnums.add("武汉市3");
		cityEnums.add("武汉市4");
		cityEnums.add("天津市1");
		cityEnums.add("天津市2");
		cityEnums.add("天津市3");
		cityEnums.add("天津市4");
		cityEnums.add("青岛市1");
		cityEnums.add("青岛市2");
		cityEnums.add("青岛市3");
		cityEnums.add("青岛市4");
		cityEnums.add("哈尔滨市1");
		cityEnums.add("哈尔滨市2");
		cityEnums.add("哈尔滨市3");
		cityEnums.add("乌鲁木齐1");
		cityEnums.add("乌鲁木齐2");
		cityEnums.add("乌鲁木齐3");
		cityEnums.add("乌鲁木齐4");
		
		cityNum = cityEnums.size();
	}
	
	private int uniqueId = 0;
	protected Map<String,String> genData(int sellerId,int buyerId) {
		Random random = new Random();
		random.setSeed(random.nextLong() ^ System.currentTimeMillis() ^ hashCode() ^ Runtime.getRuntime().freeMemory());
		DecimalFormat dateFormat = new DecimalFormat("00");
		DecimalFormat yearFormat = new DecimalFormat("198");
		
		Map<String,String> data = new HashMap<String,String>();
		data.put("id", (++uniqueId) + "");
		data.put("s_id", sellerId + "");
		data.put("b_id", buyerId + "");
		data.put("nick", "nick_" + buyerId);
		data.put("level", buyerId%10 + "");
		data.put("bir", yearFormat.format(random.nextInt(10)) + dateFormat.format(random.nextInt(13)) + dateFormat.format(random.nextInt(31)));
		data.put("sex", (buyerId%2 + 1) + "");
		data.put("pro", random.nextInt(36) + "");
		data.put("city", cityEnums.get(random.nextInt(cityNum)) + random.nextInt(1000));
		data.put("group","1|2|3|4|5");
		int count = random.nextInt(300) + 1;
		int am = random.nextInt(20000) + 300;
		data.put("count", count + "");
		data.put("am", am + "");
		data.put("a_p", (am/(count)) + "");
		data.put("l_t", "20" + dateFormat.format(random.nextInt(11)) +dateFormat.format(random.nextInt(12) + 1) + dateFormat.format(random.nextInt(30 + 1))
				+ dateFormat.format(random.nextInt(24 + 1)) + dateFormat.format(random.nextInt(59 + 1)) + dateFormat.format(random.nextInt(59 + 1)));
		data.put("source", random.nextBoolean()? "1" : "2");
		data.put("i_c", count + random.nextInt(count) + "");
		data.put("c_n", random.nextInt(100) + "");
		
		//以上是需要索引的数据，一下是指需要存储不需要索引的
		data.put("n_c", "100");
		data.put("n_a","10000");
		data.put("g_m", "20" + dateFormat.format(random.nextInt(11)) +dateFormat.format(random.nextInt(12) + 1) + dateFormat.format(random.nextInt(30 + 1))
				+ dateFormat.format(random.nextInt(24 + 1)) + dateFormat.format(random.nextInt(59 + 1)) + dateFormat.format(random.nextInt(59 + 1)));
		data.put("status", random.nextBoolean()?"1" : "-3");
		return data;
	}
	
	public static class Info {
		public int ratio;
		public int minNum;
		public int maxNum;
		public int type;
		public int[] sellerIds;
		
		public Info(int ratio, int minNum, int maxNum, int type) {
			this.ratio = ratio;
			this.minNum = minNum;
			this.maxNum = maxNum;
			this.type = type;
		}
		
		public int randomSellerId() {
			Random random = new Random();
			random.setSeed(random.nextLong() ^ System.currentTimeMillis() ^ hashCode() ^ Runtime.getRuntime().freeMemory());
			return sellerIds[random.nextInt(sellerIds.length)];
		}
	}

	public int getMaxSellerId() {
		return maxSellerId;
	}

	public void setMaxSellerId(int maxSellerId) {
		this.maxSellerId = maxSellerId;
	}

	public int getMaxBuyerId() {
		return maxBuyerId;
	}

	public void setMaxBuyerId(int maxBuyerId) {
		this.maxBuyerId = maxBuyerId;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getMaxCount() {
		return maxCount;
	}

	public void setMaxCount(int maxCount) {
		this.maxCount = maxCount;
	}

	public int[] getPool() {
		return pool;
	}

	public void setPool(int[] pool) {
		this.pool = pool;
	}

	public Iterator<Map<String, String>> getItr() {
		return itr;
	}

	public void setItr(Iterator<Map<String, String>> itr) {
		this.itr = itr;
	}
}
