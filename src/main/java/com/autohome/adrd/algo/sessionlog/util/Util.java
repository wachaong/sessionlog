package com.autohome.adrd.algo.sessionlog.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TException;
import org.json.JSONArray;
import org.json.JSONObject;

import com.autohome.adrd.algo.sessionlog.consume.NamedList;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.GeneratedMessage.Builder;
import com.autohome.adrd.algo.protobuf.AdLogOldOperation;
/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */
public class Util {
	private static ThreadLocal<SimpleDateFormat> Usformaters = new ThreadLocal<SimpleDateFormat>();
	private static ThreadLocal<SimpleDateFormat> Logformaters = new ThreadLocal<SimpleDateFormat>();
	private static ThreadLocal<SimpleDateFormat> Udataformaters = new ThreadLocal<SimpleDateFormat>();
	private static ThreadLocal<MessageDigest> Md5s = new ThreadLocal<MessageDigest>();

	public static final String CG_USER = "user";
	public static final String CG_PV = "pv";
	public static final String CG_SEARCH = "search";
	public static final String CG_ADDISPLAY = "addisplay";
	public static final String CG_CLK = "adclick";
	public static final String CG_SALE_LEADS = "saleleads";
	public static final String CG_INFO = "userinfo";
	public static final String CG_FILTER = "filter";
	public static final String CG_TAGS = "tags";
	public static final String CG_APPPV = "apppv";
	public static final String CG_BEHAVIOR = "behavior";
	
	public static final String CG_ADDISPLAY_NEW = "addisplay_new";
	public static final String CG_CLK_NEW = "adclick_new";

	public static final String OPERATION_SPLIT = "\1";
	public static final String OBJECT_SPLIT = "\2";
	public static final String FIELD_SPLIT = "\3";

	public static StringBuilder join(Collection<?> follows, String sep) {
		StringBuilder sb = new StringBuilder();

		if (follows == null || sep == null) {
			return sb;
		}

		Iterator<?> it = follows.iterator();
		while (it.hasNext()) {
			sb.append(it.next());
			if (it.hasNext()) {
				sb.append(sep);
			}
		}
		return sb;
	}
	
	public static int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
		// Short circuit equal case
		if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
			return 0;
		}

		int end1 = offset1 + length1;
		int end2 = offset2 + length2;
		for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
			int a = (buffer1[i] & 0xff);
			int b = (buffer2[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return length1 - length2;
	}

	public static boolean isBlank(String str) {
		if (str == null || str.length() == 0 || str.trim().length() == 0)
			return true;
		else
			return false;
	}

	public static boolean isNotBlank(String str) {
		return !isBlank(str);
	}

	public static int indexOf(String key, String[] schema) {
		return Arrays.asList(schema).indexOf(key);

	}

	public static String getAttr(String log, String key) {
		String regex = "\"" + key + "\"[\\p{Blank}]*:[\\p{Blank}]*((\"[\\p{Print}]*?\")|([\\p{Print}]*?))[\\p{Blank}]*[,\\}]";
		Pattern pattern = Pattern.compile(regex);
		Matcher match = pattern.matcher(log);
		if (match.find()) {
			String quoted = match.group(2);
			String valueInLog = quoted != null ? quoted.substring(1, quoted.length() - 1) : match.group(3);
			valueInLog = valueInLog.trim();
			if (Util.isNotBlank(valueInLog)) {
				return valueInLog;
			}
		}
		return null;
	}

	public static void trastoId(long v, BytesWritable data) {
		byte writeBuffer[] = null;
		if (data == null || ((writeBuffer = data.getBytes()).length != 8)) {
			throw new IllegalArgumentException("not id key BytesWritable");
		}
		writeBuffer[0] = (byte) (v >>> 56);
		writeBuffer[1] = (byte) (v >>> 48);
		writeBuffer[2] = (byte) (v >>> 40);
		writeBuffer[3] = (byte) (v >>> 32);
		writeBuffer[4] = (byte) (v >>> 24);
		writeBuffer[5] = (byte) (v >>> 16);
		writeBuffer[6] = (byte) (v >>> 8);
		writeBuffer[7] = (byte) (v >>> 0);
	}

	public static long trasfromId(BytesWritable data) {
		byte readBuffer[] = null;
		if (data == null || ((readBuffer = data.getBytes()).length < 8)) {
			throw new IllegalArgumentException("not id key BytesWritable");
		}
		return (((long) readBuffer[0] << 56) + ((long) (readBuffer[1] & 255) << 48) + ((long) (readBuffer[2] & 255) << 40) + ((long) (readBuffer[3] & 255) << 32)
				+ ((long) (readBuffer[4] & 255) << 24) + ((readBuffer[5] & 255) << 16) + ((readBuffer[6] & 255) << 8) + ((readBuffer[7] & 255) << 0));
	}

	public static long readLong(byte data[], int off) {
		if (off + 8 > data.length)
			throw new RuntimeException("");
		return (((long) data[off + 0] << 56) + ((long) (data[off + 1] & 255) << 48) + ((long) (data[off + 2] & 255) << 40) + ((long) (data[off + 3] & 255) << 32)
				+ ((long) (data[off + 4] & 255) << 24) + ((data[off + 5] & 255) << 16) + ((data[off + 6] & 255) << 8) + ((data[off + 7] & 255) << 0));
	}

	public static SimpleDateFormat usFormat() {
		SimpleDateFormat format = Usformaters.get();
		if (format == null) {
			format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
			Usformaters.set(format);
		}
		return format;
	}

	public static SimpleDateFormat logFormat() {
		SimpleDateFormat format = Logformaters.get();
		if (format == null) {
			format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Logformaters.set(format);
		}
		return format;
	}

	public static SimpleDateFormat udataFormat() {
		SimpleDateFormat format = Udataformaters.get();
		if (format == null) {
			format = new SimpleDateFormat("yyyyMMdd");
			Udataformaters.set(format);
		}
		return format;
	}

	public static String getMD5(String s) {
		try {
			MessageDigest md5 = Md5s.get();
			if (md5 == null) {
				md5 = MessageDigest.getInstance("MD5");
				Md5s.set(md5);
			}
			byte[] byteArray = s.getBytes("ISO-8859-1");
			byte[] md5Bytes = md5.digest(byteArray);
			StringBuffer hexValue = new StringBuffer();
			for (int i = 0; i < md5Bytes.length; i++) {
				int val = ((int) md5Bytes[i]) & 0xff;
				if (val < 16)
					hexValue.append("0");
				hexValue.append(Integer.toHexString(val));
			}
			return hexValue.toString();
		} catch (Exception e) {
			return null;
		}
	}

	public static void main(String args[]) throws IOException, ParseException {
		
	}
}
