package com.taobao.terminator.common;

public class ServiceType {
	
	public static final String ROLE_MERGER   = "merger";
	public static final String ROLE_READER   = "readr";
	public static final String ROLE_WRITER   = "writer";
	public static final String ROLE_INDEX_WRITER  = "indexWriter";
	
	private int code;
	private String type;
	
	private ServiceType(){}
	private ServiceType(int code,String role){
		this.code = code;
		this.type = role;
	}
	
	public static final ServiceType reader = new ServiceType(1,ROLE_READER);
	public static final ServiceType writer = new ServiceType(2,ROLE_WRITER);
	public static final ServiceType merger = new ServiceType(3,ROLE_MERGER);
	
	public String getType(){
		return this.type;
	}
	public int getCode(){
		return this.code;
	}
	
	public String toString(){
		return type;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + code;
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ServiceType other = (ServiceType) obj;
		if (code != other.code)
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}
	
}
