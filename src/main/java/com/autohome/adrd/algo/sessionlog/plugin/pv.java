package com.autohome.adrd.algo.sessionlog.plugin;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.sessionlog.interfaces.AbstractProtobuf;
import com.autohome.adrd.algo.sessionlog.interfaces.ExtractorEntry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.GeneratedMessage.Builder;

public class pv extends AbstractProtobuf {
	
	@Override
	public Builder<?> getProtoBuilder() {
		Builder<?> builderProcessor = PvlogOperation.AutoPVInfoList.newBuilder();
		return builderProcessor;
	}

	@Override
	public void ProtoAdd(Builder<?> builder, byte[] data) throws InvalidProtocolBufferException {
		PvlogOperation.AutoPVInfoList.Builder builder_tmp = (PvlogOperation.AutoPVInfoList.Builder)builder;
		builder_tmp.addPvlog(PvlogOperation.AutoPVInfo.parseFrom(data));
	}

	@Override
	public Message extract(ExtractorEntry entry, String line) {
		
		String[] tokens=line.replace("\\t", "\t").split("\t",-1);
		if(tokens.length!=32) 
			return null;

		if(tokens[29].length() > 36)
			entry.setUserKey(tokens[29].substring(0,36));
		else
			entry.setUserKey(tokens[29]);
		
		try {
			Date d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(tokens[0]);
			entry.setTimestamp(d.getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//long timestamp = d.getTime();//Long.parseLong(tokens[0]);
		//entry.setTimestamp(timestamp);

		PvlogOperation.AutoPVInfo.Builder builderExtractor = PvlogOperation.AutoPVInfo.newBuilder();
		builderExtractor.clear();
		builderExtractor.setVisittime(tokens[0]);
		builderExtractor.setLogid(tokens[1]);
		builderExtractor.setSite1Id(tokens[2]);
		builderExtractor.setSite1Name(tokens[3]);
		builderExtractor.setSite2Id(tokens[4]);
		builderExtractor.setSite2Name(tokens[5]);
		builderExtractor.setSite3Id(tokens[6]);
		builderExtractor.setSite3Name(tokens[7]);
		builderExtractor.setObjectid(tokens[8]);
		builderExtractor.setCurdomain(tokens[9]);
		builderExtractor.setCururl(tokens[10]);
		builderExtractor.setReferdomain(tokens[11]);
		builderExtractor.setReferurl(tokens[12]);
		builderExtractor.setIp(tokens[13]);
		builderExtractor.setSeriesid(tokens[14]);
		builderExtractor.setSeriesname(tokens[15]);
		builderExtractor.setSpecid(tokens[16]);
		builderExtractor.setSpecname(tokens[17]);
		builderExtractor.setCateid(tokens[18]);
		builderExtractor.setSubcateid(tokens[19]);
		builderExtractor.setJbid(tokens[20]);
		builderExtractor.setSearchword(tokens[21]);
		builderExtractor.setUserid(tokens[22]);
		builderExtractor.setPvareaid(tokens[23]);
		builderExtractor.setDealerid(tokens[24]);
		builderExtractor.setProvinceid(tokens[25]);
		builderExtractor.setProvincename(tokens[26]);
		builderExtractor.setCityid(tokens[27]);
		builderExtractor.setCityname(tokens[28]);
		builderExtractor.setCoockies(tokens[29]);
		builderExtractor.setSessionid(tokens[30]);
		builderExtractor.setUseragent(tokens[31]);
		return builderExtractor.build();
	}
}