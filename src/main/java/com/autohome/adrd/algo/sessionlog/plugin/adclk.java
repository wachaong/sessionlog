package com.autohome.adrd.algo.sessionlog.plugin;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.autohome.adrd.algo.sessionlog.interfaces.AbstractProtobuf;
import com.autohome.adrd.algo.sessionlog.interfaces.ExtractorEntry;
import com.autohome.adrd.algo.protobuf.AdLogOperation;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.GeneratedMessage.Builder;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */

public class adclk extends AbstractProtobuf {
	
	@Override
	public Builder<?> getProtoBuilder() {
		Builder<?> builderProcessor = AdLogOperation.AdCLKInfoList.newBuilder();
		return builderProcessor;
	}

	@Override
	public void ProtoAdd(Builder<?> builder, byte[] data) throws InvalidProtocolBufferException {
		AdLogOperation.AdCLKInfoList.Builder builder_tmp = (AdLogOperation.AdCLKInfoList.Builder)builder;
		builder_tmp.addClk(AdLogOperation.AdCLKInfo.parseFrom(data));
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
	public Message extract(ExtractorEntry entry, String line){
		String[] tokens=line.split("\t",-1);
		if(tokens.length < 42) 
		{			
			return null;
		}
		
		String cookie = getCookie(tokens[9],tokens[6],tokens[14]);
		long timestamp=parsetime(tokens[0]);
		
		entry.setUserKey(cookie);
		entry.setTimestamp(timestamp);
		
		AdLogOperation.AdCLKInfo.Builder builderExtractor = AdLogOperation.AdCLKInfo.newBuilder();
		builderExtractor.clear();
		builderExtractor.setClktime(tokens[0]);
		builderExtractor.setVersion(tokens[1]);
		builderExtractor.setPvid(tokens[2]);
		builderExtractor.setClkid(tokens[3]);
		builderExtractor.setImpresstime(tokens[4]);
		builderExtractor.setImpressip(tokens[5]);		
		builderExtractor.setClkip(tokens[6]);	
		builderExtractor.setProvince(tokens[7]);	
		builderExtractor.setCity(tokens[8]);	
		builderExtractor.setPvcookie(tokens[9]);	
		builderExtractor.setClkcookie(tokens[10]);	
		builderExtractor.setHost(tokens[11]);	
		builderExtractor.setPvuid(tokens[12]);
		builderExtractor.setClkuid(tokens[13]);	
		builderExtractor.setUseragent(tokens[14]);	
		builderExtractor.setPageid(tokens[15]);	
		builderExtractor.setLan(tokens[16]);
		builderExtractor.setOsversion(tokens[17]);
		builderExtractor.setAppid(tokens[18]);
		builderExtractor.setBrand(tokens[19]);
		builderExtractor.setScreenWidth(tokens[20]);
		builderExtractor.setScreenHight(tokens[21]);
		builderExtractor.setLatitude(tokens[22]);
		builderExtractor.setLongitude(tokens[23]);
		builderExtractor.setPsid(tokens[24]);
		builderExtractor.setPlatform(tokens[25]);
		builderExtractor.setReferer(tokens[26]);
		builderExtractor.setDesturl(tokens[27]);
		builderExtractor.setSiteid(tokens[28]);
		builderExtractor.setCategoryid(tokens[29]);
		builderExtractor.setSubcategoryid(tokens[30]);
		builderExtractor.setCampaignid(tokens[31]);
		builderExtractor.setGroupid(tokens[32]);
		builderExtractor.setCreativeid(tokens[33]);
		builderExtractor.setBehavior(tokens[34]);
		builderExtractor.setFrame(tokens[35]);
		builderExtractor.setPageinfo(tokens[36]);
		builderExtractor.setMatchuserinfo(tokens[37]);
		builderExtractor.setMatchpageinfo(tokens[38]);
		builderExtractor.setCreativesize(tokens[39]);
		builderExtractor.setCreativeform(tokens[40]);
		builderExtractor.setFilter(tokens[41]);	
		
		if(tokens.length >= 43)
			builderExtractor.setCarouselid(tokens[42]);
		
		return builderExtractor.build();
	}
}		

