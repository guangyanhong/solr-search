package org.taobao.terminator.client.search4zhaopin;

public enum BusiScopeTypeEnum {
	COMPUTER_FIT("电脑/配件", 1), PHONE_DIGITAL("手机/数码", 2),
	Cloth("男女服装", 3), BAG("箱包/鞋帽", 4),
	JEWELRY("首饰/奢侈品", 5), COSMETIC("化妆品", 6),
	GOODS("家居百货", 7), BOOK_VIDEO("书籍音像", 8),
	MOTHER_BABY("母婴/儿童", 9), SPORTS("运动/户外", 10),
	OTHER("其他", 11);
	private final int value;

	private final String meaning;

	public int getValue() {
		return value;
	}

	public String getMeaning() {
		return meaning;
	}

	BusiScopeTypeEnum(String meaning,int value){
		this.value = value;
		this.meaning = meaning;
	}

	public static BusiScopeTypeEnum valueOf(int value){
		for(BusiScopeTypeEnum e: BusiScopeTypeEnum.values()){
			if(e.getValue() == value){
				return e;
			}
		}
		return null;
	}
}
