package com.autohome.adrd.algo.sessionlog.interfaces;

import com.autohome.adrd.algo.sessionlog.util.Util;
/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */

public class ProcessorEntry {
		
	private long timestamp = 0;
	private byte data[] = null;
	int length = 0;

	public ProcessorEntry(long time, byte data[], int length) {
		if (time < 0 ) {
			throw new RuntimeException("Conrrupt mapper load data 1");
		}
		if ( data == null ) {
			throw new RuntimeException("Conrrupt mapper load data 2");
		}
		this.timestamp = time;
		this.data = data;
		this.length = length;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimeStamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public byte[] getData() {
		return data;
	}
	
	public int getLength() {
		return length;
	}

	public void setData(byte[] data,int length) {
		for(int i = 0; i < length; i++) {
			this.data[i+9] = data[i];
		}
	}
	
	public boolean less(ProcessorEntry pe) {
		if(this.getTimestamp() < pe.getTimestamp())
			return true;
		if(this.getTimestamp() == pe.getTimestamp() && Util.compareTo(this.getData(), 9, this.getData().length - 9, pe.getData(), 9, pe.getData().length - 9) < 0 )
			return true;
		return false;
		
	}
	
	public boolean greater(ProcessorEntry pe) {
		if(this.getTimestamp() > pe.getTimestamp())
			return true;
		if(this.getTimestamp() == pe.getTimestamp() && Util.compareTo(this.getData(), 9, this.getData().length - 9, pe.getData(), 9, pe.getData().length - 9) > 0 )
			return true;
		return false;
		
	}
	
	public boolean equals(ProcessorEntry pe) {
		if(pe.getTimestamp() == this.getTimestamp() && Util.compareTo(this.getData(), 9, this.getData().length - 9, pe.getData(), 9, pe.getData().length - 9) == 0 )
			return true;
		return false;
		
	}
	
	
}
