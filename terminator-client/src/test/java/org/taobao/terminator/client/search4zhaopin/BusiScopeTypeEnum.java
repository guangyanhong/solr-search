package org.taobao.terminator.client.search4zhaopin;

public enum BusiScopeTypeEnum {
	COMPUTER_FIT("����/���", 1), PHONE_DIGITAL("�ֻ�/����", 2),
	Cloth("��Ů��װ", 3), BAG("���/Ьñ", 4),
	JEWELRY("����/�ݳ�Ʒ", 5), COSMETIC("��ױƷ", 6),
	GOODS("�ҾӰٻ�", 7), BOOK_VIDEO("�鼮����", 8),
	MOTHER_BABY("ĸӤ/��ͯ", 9), SPORTS("�˶�/����", 10),
	OTHER("����", 11);
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
