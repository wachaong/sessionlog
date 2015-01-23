package com.autohome.adrd.algo.sessionlog.plugin;

import com.autohome.adrd.algo.sessionlog.interfaces.AbstractProtobuf;
import com.autohome.adrd.algo.sessionlog.interfaces.ExtractorEntry;
import com.autohome.adrd.algo.protobuf.AdLogOldOperation;
import com.google.protobuf.GeneratedMessage.Builder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */

public class adoldpv extends AbstractProtobuf {
	
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
		Builder<?> builderProcessor = AdLogOldOperation.AdPVOldInfoList.newBuilder();
		return builderProcessor;
	}

	@Override
	public void ProtoAdd(Builder<?> builder, byte[] data) throws InvalidProtocolBufferException {
		AdLogOldOperation.AdPVOldInfoList.Builder builder_tmp = (AdLogOldOperation.AdPVOldInfoList.Builder)builder;
		builder_tmp.addPv(AdLogOldOperation.AdPVOldInfo.parseFrom(data));
	}

	@Override
	public Message extract(ExtractorEntry entry, String line) {
		String[] tokens=line.split("\t",-1);
		if(tokens.length!=11) 
			return null;
		
		String cookie = getCookie(tokens[9],tokens[4],tokens[3],tokens[0],tokens[8]);
		entry.setUserKey(cookie);
		
		long timestamp = Long.parseLong(tokens[6]);
		entry.setTimestamp(timestamp);
		
		AdLogOldOperation.AdPVOldInfo.Builder builderExtractor = AdLogOldOperation.AdPVOldInfo.newBuilder();
		builderExtractor.clear();
		builderExtractor.setCookie(tokens[9]);
		builderExtractor.setAdtype(tokens[1]);
		builderExtractor.setCreativeid(tokens[0]);
		builderExtractor.setPageid(tokens[2]);
		builderExtractor.setPsid(tokens[3]);
		builderExtractor.setIp(tokens[4]);
		builderExtractor.setRegionid(tokens[5]);
		builderExtractor.setVtime(tokens[6]);
		builderExtractor.setCarid(tokens[7]);
		builderExtractor.setPvid(tokens[8]);
		builderExtractor.setReqid(tokens[10]);
		return builderExtractor.build();
	}

}
