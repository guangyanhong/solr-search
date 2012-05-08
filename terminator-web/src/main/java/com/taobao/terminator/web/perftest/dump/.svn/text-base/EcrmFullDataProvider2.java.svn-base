package com.taobao.terminator.web.perftest.dump;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.data.DataProvider;

/**
 * 按照 卖家-> 买家数量分布比例生成数据
 * 
 * @author yusen
 *
 */
public class EcrmFullDataProvider2 implements DataProvider{
	private final Log log = LogFactory.getLog(EcrmFullDataProvider2.class);
	
	private static final int MAX_SELLER_ID = 5000;
	private static final int BUYER_NUM_PER_SELLER = 10000;
	private static final int MAX_BUYER_ID = 100000000;
	
	private int maxBuyerId = MAX_BUYER_ID;
	private int maxSellerId = MAX_SELLER_ID;
	private int buyerNumPerSeller = BUYER_NUM_PER_SELLER;
	
	
	private static final String[] level_enum = new String[]{"1","2","3","4","5"};
	private static final String[] sex_enum   = new String[]{"1","2"};
	private static final String[] source_enum= new String[]{"1","2"};
	private static final String[] group_enum = new String[]{"1|2|3","2|3|5","3|4|7","4|5|8","5|6|9|10","6|7|5","7|8","8|9","9|10","10|1|4"}; 
	
	
	
	private BlockingQueue<Map<String,String>> buff = new LinkedBlockingQueue<Map<String,String>>(10000);
	
	@Override
	public boolean hasNext() throws Exception {
		return buff.peek() != null || t.isAlive();
	}
	
	ProductThread t = null;
	@Override
	public void init() throws Exception {
		t = new ProductThread();
		t.setName("Full-Dump-DataProvider-Thread");
		t.setDaemon(true);
		t.start();
	}

	@Override
	public Map<String, String> next() throws Exception {
		return buff.take();
	}

	@Override
	public void close() throws Exception {
		buff.clear();
	}
	
	
	private class ProductThread extends Thread {
		public void run() {
			for (int sellerId = 1; sellerId <= maxSellerId; sellerId++) {
				int count = 0;
				outer:while(true) {
					for(String level : level_enum) {
						for(String sex : sex_enum) {
							for(String source : source_enum) {
								for(String group : group_enum) {
									Map<String,String> d = genData(sellerId, level, sex, source, group);
									try {
										buff.put(d);
										if(++count >= buyerNumPerSeller) {
											break outer;
										}
									} catch (InterruptedException e) {
										log.error(e,e);
										throw new RuntimeException(e);
									}
								}
							}
						}
					}
				}
			}
		}
	}
	
	private long uniqueId = 0;
	
	private Map<String,String> genData(long sellerId,String level,String sex,String source,String group) {
		Random random = new Random();
		random.setSeed(random.nextLong() ^ System.currentTimeMillis() ^ hashCode() ^ Runtime.getRuntime().freeMemory());
		DecimalFormat dateFormat = new DecimalFormat("00");
		DecimalFormat yearFormat = new DecimalFormat("198");
		
		int buyerId = random.nextInt(maxBuyerId);
		
		Map<String,String> data = new HashMap<String,String>();
		data.put("id", (++uniqueId) + "");
		data.put("s_id", sellerId + "");
		data.put("b_id", buyerId + "");
		data.put("nick", "nick_" + buyerId);
		data.put("level", level);
		data.put("bir", yearFormat.format(random.nextInt(10)) + dateFormat.format(random.nextInt(13)) + dateFormat.format(random.nextInt(31)));
		data.put("sex", sex);
		data.put("source", source);
		data.put("group",group);
		data.put("status", random.nextBoolean()?"1" : "-3");
		data.put("pro", random.nextInt(36) + "");
		data.put("city", cityEnums.get(random.nextInt(cityNum)) + random.nextInt(1000));
		
		int count = random.nextInt(Short.MAX_VALUE) + 1;
		int am = random.nextInt(20000) + Short.MAX_VALUE;
		data.put("count", count + "");
		data.put("bir", (random.nextInt(12) + 1)+"");
		data.put("i_c", random.nextInt(Short.MAX_VALUE) + "");
		data.put("c_n", random.nextInt(Short.MAX_VALUE) + "");
		
		data.put("am", am + "");
		data.put("a_p", (am/(count)) + "");
		data.put("l_t", "20" + dateFormat.format(random.nextInt(11)) +dateFormat.format(random.nextInt(12) + 1) + dateFormat.format(random.nextInt(31)));
		data.put("n_c", "100");
		data.put("n_a","10000");
		data.put("g_m", "20" + dateFormat.format(random.nextInt(11)) +dateFormat.format(random.nextInt(12) + 1) + dateFormat.format(random.nextInt(31))
				+ dateFormat.format(random.nextInt(24)) + dateFormat.format(random.nextInt(60)) + dateFormat.format(random.nextInt(60)));
		data.put("status", random.nextBoolean()?"1" : "-3");
		return data;
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
	
	
	
	public int getMaxBuyerId() {
		return maxBuyerId;
	}

	public void setMaxBuyerId(int maxBuyerId) {
		this.maxBuyerId = maxBuyerId;
	}

	public int getMaxSellerId() {
		return maxSellerId;
	}

	public void setMaxSellerId(int maxSellerId) {
		this.maxSellerId = maxSellerId;
	}

	public int getBuyerNumPerSeller() {
		return buyerNumPerSeller;
	}

	public void setBuyerNumPerSeller(int buyerNumPerSeller) {
		this.buyerNumPerSeller = buyerNumPerSeller;
	}

	public BlockingQueue<Map<String, String>> getBuff() {
		return buff;
	}

	public void setBuff(BlockingQueue<Map<String, String>> buff) {
		this.buff = buff;
	}

	public ProductThread getT() {
		return t;
	}

	public void setT(ProductThread t) {
		this.t = t;
	}

	public long getUniqueId() {
		return uniqueId;
	}

	public void setUniqueId(long uniqueId) {
		this.uniqueId = uniqueId;
	}

	public Log getLog() {
		return log;
	}

	public static void main(String[] args) throws Exception{
		EcrmFullDataProvider2 dp = new EcrmFullDataProvider2();
		dp.setMaxSellerId(5);
		dp.setBuyerNumPerSeller(1);
		dp.init();
		int count = 0;
		while(dp.hasNext()) {
			dp.next();
			System.out.println(++count);
		}
	}
}
