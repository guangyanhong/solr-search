package org.taobao.terminator.client.search4zhaopin;

public enum CatalogTypeEnum {
	CUSTOMSERVICE("客服", 0), Photograph("美工/P图/摄影", 1), CHARACTEREDIT("文字编辑", 2), PACKAGE("打包发货", 3), MARKET("营销/推广", 4), AFTERMARKET("售后服务", 5), PLANEMODEL("平面模特", 6), OTHER("其他", 7);

	private final int value;

	private final String meaning;

	public int getValue() {
		return value;
	}

	public String getMeaning() {
		return meaning;
	}

	CatalogTypeEnum(String meaning, int value) {
		this.value = value;
		this.meaning = meaning;
	}

	public static CatalogTypeEnum valueOf(int value) {
		for (CatalogTypeEnum e : CatalogTypeEnum.values()) {
			if (e.getValue() == value) {
				return e;
			}
		}
		return null;
	}
}
