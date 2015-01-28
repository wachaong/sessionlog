package com.autohome.adrd.algo.sessionlog.plugin;

import com.autohome.adrd.algo.sessionlog.interfaces.AbstractProtobuf;
import com.autohome.adrd.algo.sessionlog.interfaces.ExtractorEntry;
import com.autohome.adrd.algo.protobuf.AdLogOldOperation;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.GeneratedMessage.Builder;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */

public class adoldclk extends AbstractProtobuf {

	private String getCookie(String sessionid, String ip, String psid, String CreativeId, String pvid)
	{
		String cookie = null;
		if( (!sessionid.equals("-1")) && (!sessionid.equals("null")) )
		{
			cookie = sessionid.split("%7C",-1)[0].split("\\|\\|",-1)[0];
		}
		else
		{
			cookie = ip + "_" + psid + "_" + CreativeId + "_" + pvid;
		}
		return cookie;
	}
	
	@Override
	public Builder<?> getProtoBuilder() {
		Builder<?> builderProcessor = AdLogOldOperation.AdCLKOldInfoList.newBuilder();
		return builderProcessor;
	}

	@Override
	public void ProtoAdd(Builder<?> builder, byte[] data) throws InvalidProtocolBufferException {
		AdLogOldOperation.AdCLKOldInfoList.Builder builder_tmp = (AdLogOldOperation.AdCLKOldInfoList.Builder)builder;
		builder_tmp.addClk(AdLogOldOperation.AdCLKOldInfo.parseFrom(data));
	}

	@Override
	public Message extract(ExtractorEntry entry, String line){
		String[] tokens=line.split("\t",-1);
		if(tokens.length!=12) 
			return null;
		
		String cookie = getCookie(tokens[10],tokens[4],tokens[3],tokens[0],tokens[9]);
		long timestamp = Long.parseLong(tokens[6]);
		
		entry.setUserKey(cookie);
		entry.setTimestamp(timestamp);
		
		AdLogOldOperation.AdCLKOldInfo.Builder builderExtractor = AdLogOldOperation.AdCLKOldInfo.newBuilder();
		builderExtractor.clear();
		builderExtractor.setCookie(tokens[10]);
		builderExtractor.setCreativeid(tokens[0]);
		builderExtractor.setAdtype(tokens[1]);
		builderExtractor.setPageid(tokens[2]);
		builderExtractor.setPsid(tokens[3]);
		builderExtractor.setClkip(tokens[4]);
		builderExtractor.setRegionid(tokens[5]);
		builderExtractor.setClktime(tokens[6]);
		builderExtractor.setFrame(tokens[7]);
		builderExtractor.setReferer(tokens[8]);
		builderExtractor.setPvid(tokens[9]);
		builderExtractor.setFreqid(tokens[11]);
		
		return builderExtractor.build();
	}
}		

