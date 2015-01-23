package com.autohome.adrd.algo.sessionlog.config;
/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */
public class PathConfig {
	
	private String location = null;
	private String recordReader = null;
	private String op = null;
	//private String extractor = null;
	//private String processor = null;
	
	public String getRecordReader() {
		return recordReader;
	}
	
	public void setRecordReader(String recordReader) {
		this.recordReader = recordReader;
	}
	
	/*
	public String getExtractor() {
		return extractor;
	}
	
	public void setExtractor(String extractor) {
		this.extractor = extractor;
	}
	*/
	
	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}
	
	public String getOp() {
		return op;
	}
	
	public void setOp(String op) {
		this.op = op;
	}
	
	/*
	public String getProcessor() {
		return processor;
	}
	
	public void setProcessor(String processor) {
		this.processor = processor;
	}
	*/
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("{path=").append(location).append(", recordReader=").append(recordReader);
		sb.append(", op=").append(op).append("}");
		return sb.toString();
	}

}
