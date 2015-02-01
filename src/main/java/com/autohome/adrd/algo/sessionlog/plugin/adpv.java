package com.autohome.adrd.algo.sessionlog.plugin;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.autohome.adrd.algo.sessionlog.interfaces.AbstractProtobuf;
import com.autohome.adrd.algo.sessionlog.interfaces.ExtractorEntry;
import com.autohome.adrd.algo.protobuf.AdLogOperation;
import com.google.protobuf.GeneratedMessage.Builder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */

public class adpv extends AbstractProtobuf {
		
	@Override
	public Builder<?> getProtoBuilder() {
		Builder<?> builderProcessor = AdLogOperation.AdPVInfoList.newBuilder();
		return builderProcessor;
	}

	@Override
	public void ProtoAdd(Builder<?> builder, byte[] data) throws InvalidProtocolBufferException {
		AdLogOperation.AdPVInfoList.Builder builder_tmp = (AdLogOperation.AdPVInfoList.Builder)builder;
		builder_tmp.addPv(AdLogOperation.AdPVInfo.parseFrom(data));
	}

	private long parsetime(String time) {
		long vtime=0;		
		SimpleDateFormat format =   new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );  	     
	    Date date;
		try {
			date = format.parse(time);
			vtime=date.getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  	   	    
	    return vtime;
	}
	
	private String getCookie(String sessionid,String ip,String ua) {
		String cookie="";
		if ( ! ((sessionid.equals(null) || (sessionid.equals("")) || (sessionid.equals("null")) ))) {
				cookie=sessionid;			
			}
		else {
			cookie= sessionid+"_"+ip+"_"+ua;
		}		
		return cookie;		
	}
	
	@Override
	public Message extract(ExtractorEntry entry, String line) {
		String[] tokens=line.split("\t",-1);
		if(tokens.length < 40) 
			return null;
		
		String cookie = getCookie(tokens[15],tokens[16],tokens[19]);
		entry.setUserKey(cookie);
		
		long timestamp=parsetime(tokens[0]);
		entry.setTimestamp(timestamp);
		
		AdLogOperation.AdPVInfo.Builder builderExtractor = AdLogOperation.AdPVInfo.newBuilder();
		builderExtractor.setVtime(tokens[0]);
		builderExtractor.setVersion(tokens[1]);
		builderExtractor.setPath(tokens[2]);
		builderExtractor.setPageid(tokens[3]);
		builderExtractor.setReqid(tokens[4]);
		builderExtractor.setPvid(tokens[5]);		
		builderExtractor.setBucketid(tokens[6]);	
		builderExtractor.setRt(tokens[7]);	
		builderExtractor.setPsid(tokens[8]);	
		builderExtractor.setPlatform(tokens[9]);	
		builderExtractor.setRefferurl(tokens[10]);	
		builderExtractor.setPageurl(tokens[11]);	
		builderExtractor.setSiteid(tokens[12]);
		builderExtractor.setCategoryid(tokens[13]);	
		builderExtractor.setSubcategoryid(tokens[14]);	
		builderExtractor.setSessionid(tokens[15]);	
		builderExtractor.setIp(tokens[16]);
		builderExtractor.setProvince(tokens[17]);
		builderExtractor.setCity(tokens[18]);
		builderExtractor.setUa(tokens[19]);
		builderExtractor.setUid(tokens[20]);
		builderExtractor.setUserinfo(tokens[21]);
		builderExtractor.setPageinfo(tokens[22]);
		builderExtractor.setLan(tokens[23]);
		builderExtractor.setOsversion(tokens[24]);
		builderExtractor.setAppid(tokens[25]);
		builderExtractor.setBrand(tokens[26]);
		builderExtractor.setScreenWidth(tokens[27]);
		builderExtractor.setScreenHight(tokens[28]);
		builderExtractor.setLatitude(tokens[29]);
		builderExtractor.setLongitude(tokens[30]);
		builderExtractor.setCampaignid(tokens[31]);
		builderExtractor.setGroupid(tokens[32]);
		builderExtractor.setCreativeid(tokens[33]);
		builderExtractor.setSellmodel(tokens[34]);
		builderExtractor.setCreativesize(tokens[35]);
		builderExtractor.setCreativeform(tokens[36]);
		builderExtractor.setMatchuserinfo(tokens[37]);
		builderExtractor.setMatchpageinfo(tokens[38]);
		builderExtractor.setFilter(tokens[39]);	
		
		if(tokens.length >= 41)
			builderExtractor.setCarouselid(tokens[40]);
		if(tokens.length >= 42)
			builderExtractor.setAlgoDetail(tokens[41]);
		if(tokens.length >= 43)
			builderExtractor.setPositionLabel(tokens[42]);
		if(tokens.length >= 44)
			builderExtractor.setPositionWidthHeight(tokens[43]);			
		return builderExtractor.build();
	}

}
