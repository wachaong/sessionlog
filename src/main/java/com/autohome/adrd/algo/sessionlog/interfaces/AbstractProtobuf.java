package com.autohome.adrd.algo.sessionlog.interfaces;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.autohome.adrd.algo.sessionlog.interfaces.ProcessorEntry;
import com.autohome.adrd.algo.sessionlog.interfaces.ResultEntry;
import com.google.protobuf.GeneratedMessage.Builder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */
public abstract class AbstractProtobuf{
	
	 private ArrayList<ExtractorEntry> entryList;
	 private ByteArrayOutputStream bufferExtractor;
	 private ByteArrayOutputStream bufferProcessor;
	 protected Builder<?> builderProcessor;
	 
	 public AbstractProtobuf(){
	 	entryList = new ArrayList<ExtractorEntry>();
	 	bufferExtractor = new ByteArrayOutputStream(512);
		bufferProcessor = new ByteArrayOutputStream(512);
		builderProcessor = getProtoBuilder();
	 }
	 
	 abstract public void ProtoAdd(Builder<?> builder, byte[] data) throws InvalidProtocolBufferException;
	 
	 abstract public Message extract(ExtractorEntry entry,String line);
	 
	 abstract public Builder<?> getProtoBuilder();
	 
	 public List<ExtractorEntry> extract(String strs){
		entryList.clear();
    	bufferExtractor.reset();
    	
    	ExtractorEntry entry = new ExtractorEntry();
    	Message msg = extract(entry,strs);

    	if(msg == null)
			return entryList;
    	
		try {			
			bufferExtractor.write(msg.toByteArray());
			entry.setValue(bufferExtractor.toByteArray());			
			entryList.add(entry);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			bufferExtractor.reset();
			e.printStackTrace();
		}
		return entryList;
	 };
	 
	 public ResultEntry process(Iterator<ProcessorEntry> it){
		ResultEntry re = new ResultEntry();
		bufferProcessor.reset();
		builderProcessor.clear();
		
		while (it.hasNext()) {
			ProcessorEntry entry = it.next();
			bufferProcessor.write(entry.getData(), 9, entry.getLength() - 9);
			try{
				ProtoAdd(builderProcessor, bufferProcessor.toByteArray());
			}catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			bufferProcessor.reset();
		}
		re.setData(builderProcessor.build().toByteArray());
		return re;
	 };
	 	 
}		

