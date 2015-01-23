package com.autohome.adrd.algo.sessionlog.sessionlog_verify;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/**
 * Common Data And Function
 * 
 * @author [wangchao]
 */
public class CommonDataAndFunc {
	
	public static final String TAB = "\t";
	
	/**
	 */
	public static Set<String> readSets(String fileName, String sep, int index,
			String encoding) {
		Set<String> res = new HashSet<String>();
		if (index < 0) {
			return res;
		}

		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(fileName), encoding));
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("#")) {
					continue;
				}
				String[] arr = line.split(sep, -1);
				if (arr.length <= index) {
					continue;
				}
				res.add(arr[index]);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return res;
	}
  
	public static void main(String[] args) throws Exception {
		String url = "7B0BEBB6-56F6-46A9-5DDB-25A382F80BA8%7C%7C2014-08-04+20%3A16%3A25.12%7C%7Chao.360.cn";	
		/*
		String url2 = "7B0BEBB6-56F6-46A9-5DDB-25A382F80BA8||2014-08-04 20:16:25.12||hao.360.cn";
		String url_decode = java.net.URLDecoder.decode(url,"utf-8");
		String[] segs = url_decode.split("\\|\\|",-1);
		for(String seg: segs)
		{
			//System.out.println(seg);
		}
		System.out.println("hi");
		System.out.println(segs[0]);
		*/
		//System.out.println(segs[1]);
		//System.out.println(segs[2]);
		System.out.println(url.split("%7C",-1)[0]);
		String aa = "B1B37E6D-5FAB-68C6-25BA-D241AA2FC4FC";
		String bb = "MDNhNzQ2ZDk5ZWZlNDQ3ZTgzMDQ5OWM5YzFlM2U2YjQ=";
		System.out.println(bb.length());
		int aaa = (int) (Math.random()*50);
		System.out.println(aaa);
		//System.out.println(URLEncoder.encode(url2));
	}

}
