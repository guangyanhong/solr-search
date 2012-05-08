package com.taobao.terminator.web.perftest;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;

import com.taobao.terminator.common.protocol.AddDocumentRequest;
import com.taobao.terminator.common.protocol.DeleteByIdRequest;
import com.taobao.terminator.common.protocol.UpdateDocumentRequest;

public class RealTimeDataProvider{
	public static final int SIGN_ADD         = 1;
	public static final int SIGN_UPDATE      = 2;
	public static final int SIGN_DEL         = 3;
	public static final int TOTAL_RATIO      = 100;
	public static final int DEFAULT_START_ID = 4000 * 10000 + 1;
	
	//add update delete操作所占的比例，总数是100，一百内划分
	private int addRatio    = 20; 
	private int updateRatio = 70;
	private int delRatio    = 10;
	
	private int addNum    = 0;
	private int updateNum = 0;
	private int delNum    = 0;
	
	private int genIdTrialTime = 10;
	
	private int currentId = DEFAULT_START_ID;
	private int[] pool = new int[TOTAL_RATIO];
	
	private BitSet dels = new BitSet();
	
	public void init(){
		int i = 0;
		for (i = 0; i < addRatio; i++) {
			pool[i] = SIGN_ADD;
		}
		
		int j = i + updateRatio;
		
		for (; i < j; i++) {
			pool[i] = SIGN_UPDATE;
		}

		j = i + delRatio;
		for (; i < j; i++) {
			pool[i] = SIGN_DEL;
		}
	}

	public boolean hasNext(){
		return true;
	}

	public Object next(){
		Random random = new Random();
		int sign = pool[random.nextInt(TOTAL_RATIO)];
		if(sign == SIGN_ADD) {
			addNum ++;
			return this.genAddReq();
		} else if(sign == SIGN_DEL) {
			delNum ++;
			return this.genDelReq();
		} else if(sign == SIGN_UPDATE) {
			updateNum ++;
			return this.genUpdateReq();
		} else {
			return null;
		}
	}
	
	public void reset() throws Exception {
		currentId = DEFAULT_START_ID;
		addNum    = 0;
		updateNum = 0;
		delNum    = 0;
		
		dels.clear();
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("currenId {").append(currentId).append("} \n");
		sb.append("addNum {").append(addNum).append("} \n");
		sb.append("updateNum {").append(updateNum).append("} \n");
		sb.append("delNum {").append(delNum).append("} \n");
		return sb.toString();
	}
	
	private AddDocumentRequest genAddReq() {
		AddDocumentRequest req = new AddDocumentRequest();
		int id = ++currentId;
		req.solrDoc = this.genDoc(id + "");
		return req;
	}
	
	private UpdateDocumentRequest genUpdateReq() {
		UpdateDocumentRequest req = new UpdateDocumentRequest();
		int id = this.genId(this.genIdTrialTime);
		req.solrDoc = this.genDoc(id + "");
		return req;
	}
	
	private DeleteByIdRequest genDelReq() {
		DeleteByIdRequest re = new DeleteByIdRequest();
		int id = this.genId(this.genIdTrialTime);
		re.id = id + "";
		dels.set(id);
		return re;
	}
	
	private int genId(int trialTime) {
		if(trialTime <= 0) {
			return -1;
		}
		
		Random random = new Random();
		int id = random.nextInt(this.currentId);
		if(dels.get(id)) {
			return this.genId(trialTime--);
		} else {
			return id;
		}
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
	
	private SolrInputDocument genDoc(String id) {
		SolrInputDocument doc = new SolrInputDocument();
		Random random = new Random();
		random.setSeed(random.nextLong() ^ System.currentTimeMillis() ^ hashCode() ^ Runtime.getRuntime().freeMemory());
		DecimalFormat mpFormat = new DecimalFormat("00000000");
		DecimalFormat dateFormat = new DecimalFormat("00");
		DecimalFormat yearFormat = new DecimalFormat("198");
		
		int sellerId = random.nextInt(4000);
		int buyerId  = random.nextInt(10000000);
		doc.addField("id", id);
		doc.addField("s_id", sellerId + "");
		doc.addField("b_id", buyerId + "");
		doc.addField("nick", "nick_" + buyerId);
		doc.addField("level", buyerId%10 + "");
		doc.addField("m_p", "138" + mpFormat.format(random.nextInt()));
		doc.addField("bir", yearFormat.format(random.nextInt(10)) + dateFormat.format(random.nextInt(13)) + dateFormat.format(random.nextInt(31)));
		doc.addField("sex", buyerId%3 + "");
		doc.addField("pro", random.nextInt(36) + "");
		doc.addField("city", cityEnums.get(random.nextInt(cityNum)) + random.nextInt(1000));
		doc.addField("group","1|2|3|4|6|7|8"); //TODO
		int count = random.nextInt(300) + 1;
		int am = random.nextInt(20000) + 300;
		doc.addField("count", count + "");
		doc.addField("am", am + "");
		doc.addField("a_p", (am/(count)) + "");
		doc.addField("l_t", "20" + dateFormat.format(random.nextInt(11)) +dateFormat.format(random.nextInt(12) + 1) + dateFormat.format(random.nextInt(30 + 1)));
		doc.addField("source", random.nextBoolean()? "1" : "2");
		doc.addField("i_c", count + random.nextInt(count) + "");
		doc.addField("c_n", random.nextInt(100) + "");
		
		//以上是需要索引的数据，一下是指需要存储不需要索引的
		/*doc.addField("email", "yusen123@taobao.com");
		doc.addField("add", "浙江省杭州市华星路创业大厦6楼小邮局");
		doc.addField("momo", "潮人;型男;找抽型");*/
		doc.addField("new_c", "100");
		doc.addField("new_a","10000");
		doc.addField("status", random.nextBoolean()?"1" : "-3");
		return doc;
	}
	
	public int getAddRatio() {
		return addRatio;
	}

	public void setAddRatio(int addRatio) {
		this.addRatio = addRatio;
	}

	public int getUpdateRatio() {
		return updateRatio;
	}

	public void setUpdateRatio(int updateRatio) {
		this.updateRatio = updateRatio;
	}

	public int getDelRatio() {
		return delRatio;
	}

	public void setDelRatio(int delRatio) {
		this.delRatio = delRatio;
	}

	public int getAddNum() {
		return addNum;
	}

	public void setAddNum(int addNum) {
		this.addNum = addNum;
	}

	public int getUpdateNum() {
		return updateNum;
	}

	public void setUpdateNum(int updateNum) {
		this.updateNum = updateNum;
	}

	public int getDelNum() {
		return delNum;
	}

	public void setDelNum(int delNum) {
		this.delNum = delNum;
	}

	public int getGenIdTrialTime() {
		return genIdTrialTime;
	}

	public void setGenIdTrialTime(int genIdTrialTime) {
		this.genIdTrialTime = genIdTrialTime;
	}

	public int getCurrentId() {
		return currentId;
	}

	public void setCurrentId(int currentId) {
		this.currentId = currentId;
	}

	public int[] getPool() {
		return pool;
	}

	public void setPool(int[] pool) {
		this.pool = pool;
	}

	public BitSet getDels() {
		return dels;
	}

	public void setDels(BitSet dels) {
		this.dels = dels;
	}

	public static void main(String[] args) {
		RealTimeDataProvider dp = new RealTimeDataProvider();
		dp.init();
		int count = 0;
		while(dp.hasNext()) {
			dp.next();
			if(count ++ % 100 == 0) {
				System.out.println(dp);
			}
			if(count == 10000) {
				break;
			}
		}
	}
}
