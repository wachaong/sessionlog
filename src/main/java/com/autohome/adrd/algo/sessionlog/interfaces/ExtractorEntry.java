package com.autohome.adrd.algo.sessionlog.interfaces;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */

public class ExtractorEntry {
	
	private String userKey = null;
		
	private long timestamp = -1;
	private byte data[] = null;
	
	public void setValue(byte data[]) {
		if (data == null) {
			throw new RuntimeException("Serializable data corrupt");
		}
		this.data = data;
	}
	
	public byte[] getData() {
		return data;
	}

	public String getUserKey() {
		return userKey;
	}
	
	public void setUserKey(String userKey) {
		this.userKey = userKey;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
		
	public boolean isBlank(String str) {
		if (str == null || str.length() == 0 || str.trim().length() == 0)
			return true;
		else
			return false;
	}
	
	public boolean check() {
		//if (isBlank(userKey) || timestamp < 0 
		if (timestamp < 0 || data == null) return false;
		return true;
	}
	
}
