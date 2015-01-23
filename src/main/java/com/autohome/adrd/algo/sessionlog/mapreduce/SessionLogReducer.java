package com.autohome.adrd.algo.sessionlog.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import com.autohome.adrd.algo.sessionlog.config.ConfigLoader;
import com.autohome.adrd.algo.sessionlog.config.PathConfig;
import com.autohome.adrd.algo.sessionlog.config.SessionLogConfig;
import com.autohome.adrd.algo.sessionlog.interfaces.AbstractProtobuf;
import com.autohome.adrd.algo.sessionlog.interfaces.PriorityQueue;
import com.autohome.adrd.algo.sessionlog.interfaces.ProcessorEntry;
import com.autohome.adrd.algo.sessionlog.interfaces.ResultEntry;
import com.autohome.adrd.algo.sessionlog.util.Util;
/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */
public class SessionLogReducer extends Reducer<Text, BytesWritable, NullWritable, BytesRefArrayWritable> {
//public class SessionLogReducer extends Reducer<Text, BytesWritable, NullWritable, Text> {

	private SessionLogConfig config = null;
	private HashMap<Byte, PriorityQueue> queues = null;
	private HashMap<Byte, AbstractProtobuf> processors = null;
	
	private ArrayList <String> schema =new ArrayList<String>();
	public static final BytesWritable emptyValue = new BytesWritable();
	private static final Log LOG = LogFactory.getLog(SessionLogReducer.class);
	
	@SuppressWarnings("unchecked")
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		config = ConfigLoader.loadConfig(conf);
		if (config == null || !config.check())
			throw new IOException("sessionlog.config logic error");

		queues = new HashMap<Byte, PriorityQueue>();
		processors = new HashMap<Byte, AbstractProtobuf>();
		
		for (int i = 1; i <= config.getPathConfigs().size(); i++) {
			PathConfig pathConfig = config.getPathConfigs().get(i-1);
			schema.add(pathConfig.getOp());
			queues.put((byte)i, new PriorityQueue());
			try {
				String processor = "com.autohome.adrd.algo.sessionlog.plugin." + pathConfig.getOp();
				Class<AbstractProtobuf> processClass = (Class<AbstractProtobuf>)Class.forName(processor);
				processors.put((byte)i, (AbstractProtobuf)ReflectionUtils.newInstance(processClass, null));
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("processor for class" + pathConfig.getOp() + " not found, " +
				"check distributedCache class path configed");
			}
		}
		schema.add("filter");
	}
	


	protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {

		String userKey = key.toString();
		boolean if_filter = false;
		Set<String> filter_info = new HashSet<String>();
		Iterator<PriorityQueue> queueIt = queues.values().iterator();
		while (queueIt.hasNext()) {
			PriorityQueue queue = queueIt.next();
			queue.clear();
		}
		System.out.println("schema size " + schema.size());
		for(BytesWritable bw : values)	//get type from mapper
		{
			BytesWritable value = new BytesWritable();
			value.set(bw);
			byte data[] = value.getBytes();			
			if (data.length <= 9)
				continue;

			PriorityQueue queue = queues.get(data[0]);
			String strOp = schema.get((int)data[0]);
			
			//单种行为>10000次，认为是异常数据，过滤之，广告数据暂时全部保留，在反作弊模块处理
			if((queue.size() > 100000 ) 
					&& ( !strOp.equals("adclick_new") && (!strOp.equals("addisplay_new")))
					&& ( !strOp.equals("adclick") && (!strOp.equals("addisplay")))
					)
			{
				if_filter = true;
				filter_info.add(strOp);
				continue;
			}
			
			ProcessorEntry entry = new ProcessorEntry(Util.readLong(data, 1), data, bw.getLength());												
			queue.add(entry); //sort by timestamp , maybe not used
		}
		
		BytesRefArrayWritable vals = new BytesRefArrayWritable(schema.size()+1);
		for (int i = 0; i < schema.size()+1; i++) {
			vals.set(i, new BytesRefWritable());
		}
				
		vals.set(0, new BytesRefWritable(userKey.getBytes()));	
			
		if(if_filter == true)
		{
			int idx = schema.indexOf("filter");
			String info = Util.join(filter_info, ",").toString();			
			vals.set(idx, new BytesRefWritable(info.getBytes()));
		}
				
		//other behavior
		Iterator<Entry<Byte, PriorityQueue>> entryIt = queues.entrySet().iterator();
		Iterator<ProcessorEntry> queueEntryIt = null;
		while (entryIt.hasNext()) {
			Entry<Byte, PriorityQueue> entry = entryIt.next();
			if (entry.getValue().size() == 0)
				continue;
			if(filter_info.contains(schema.get((int)entry.getKey())))
			{
				continue;
			}
			else
			{
				ResultEntry re = null;
				queueEntryIt = entry.getValue().iterator();
				AbstractProtobuf processor = processors.get(entry.getKey());
				if (processor != null) {
					re = processor.process(queueEntryIt);
				}
				if(re != null)
					vals.set((int)entry.getKey(), new BytesRefWritable(re.getData()));
			}
		}	
		context.write(NullWritable.get(), vals);		
	}
}