package com.autohome.adrd.algo.sessionlog.consume;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

import com.autohome.adrd.algo.protobuf.AdLogOldOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.google.protobuf.Message;

import org.apache.commons.lang.StringUtils;

/**
 * Protobuf Deserializer.
 * 
 */

public class ProtoDe extends AbstractDeserializer {

	private StructTypeInfo rowTypeInfo;
	private ObjectInspector rowOI;
	private List<String> colNames;
	private List<Object> row = new ArrayList<Object>();
	private List<TypeInfo> colTypes;
	private int index;
	Map<Class<?>, Map<String, Method>> cached = new HashMap<Class<?>, Map<String, Method>>();
	Map<Class<?>, Map<String, Method>> cachedHas = new HashMap<Class<?>, Map<String, Method>>();

	private static final class NoSuchMethod {
		@SuppressWarnings("unused")
		public final void none() {
		}
	}

	public static final Method NO_SUCH_METHOD;
	static {
		try {
			NO_SUCH_METHOD = NoSuchMethod.class.getDeclaredMethod("none");
		} catch (NoSuchMethodException e) {
			throw new RuntimeException("Can't happen", e);
		}

	}
	public static final Object[] noArgs = new Object[0];

	@Override
	public void initialize(Configuration job, Properties tbl) throws SerDeException {
		try {
			// Get a list of the table's column names.
			String colNamesStr = tbl.getProperty("columns");
			colNames = Arrays.asList(colNamesStr.split(","));

			// Get a list of TypeInfos for the columns. This list lines up with
			// the list of column names.
			String colTypesStr = tbl.getProperty("columns.types");
			colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);

			rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
			rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);

		} catch (Exception e) {
			throw new SerDeException(e);
		}
	}

	@Override
	public Object deserialize(Writable field) throws SerDeException {
		row.clear();
		for (int i = 0; i < colNames.size(); ++i)
			row.add(null);
		BytesRefArrayWritable value = (BytesRefArrayWritable) field;
		BytesRefWritable data = null;
		int colnum = 0;
		for (String str : colNames) {
			data = value.get(colnum);

			try{
				if (str.equalsIgnoreCase("adoldpv")) {
						AdLogOldOperation.AdPVOldInfoList adpv_lst = AdLogOldOperation.AdPVOldInfoList.parseFrom(data.getBytesCopy());
						index = colnum;
						ObjectInspector rowoi = createObjectInspectorWorker(colTypes.get(colnum));
						matchProtoToRowField(adpv_lst, row, rowoi, "PvList");
				} else if (str.equalsIgnoreCase("userid")) {
					String userid = new String(data.getBytesCopy());
					userid = userid == null ? "" : userid;
					row.add(colnum, StringUtils.defaultString(userid));
				}
				else if(str.equalsIgnoreCase("adoldclk")){
					AdLogOldOperation.AdCLKOldInfoList adclk_lst = AdLogOldOperation.AdCLKOldInfoList.parseFrom(data.getBytesCopy());
					index = colnum;
					ObjectInspector rowoi = createObjectInspectorWorker(colTypes.get(colnum));
					matchProtoToRowField(adclk_lst, row, rowoi, "ClkList");
				}
				else if(str.equalsIgnoreCase("pv")){
					PvlogOperation.AutoPVInfoList pv_lst = PvlogOperation.AutoPVInfoList.parseFrom(data.getBytesCopy());
					index = colnum;
					ObjectInspector rowoi = createObjectInspectorWorker(colTypes.get(colnum));
					matchProtoToRowField(pv_lst, row, rowoi, "PvlogList");
				}
			}catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			colnum++;
		}
		return row;
	}

	//
	public ObjectInspector createObjectInspectorWorker(TypeInfo ti) {
		ObjectInspector result = null;

		switch (ti.getCategory()) {
		case PRIMITIVE:
			PrimitiveTypeInfo pti = (PrimitiveTypeInfo) ti;
			result = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti.getPrimitiveCategory());
			break;
		case LIST:
			ListTypeInfo lti = (ListTypeInfo) ti;
			TypeInfo subType = lti.getListElementTypeInfo();
			result = ObjectInspectorFactory.getStandardListObjectInspector(createObjectInspectorWorker(subType));
			break;
		case STRUCT:
			StructTypeInfo sti = (StructTypeInfo) ti;
			ArrayList<String> subnames = sti.getAllStructFieldNames();
			ArrayList<TypeInfo> subtypes = sti.getAllStructFieldTypeInfos();
			ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();
			for (int i = 0; i < subtypes.size(); i++) {
				ois.add(createObjectInspectorWorker(subtypes.get(i)));
			}
			result = ObjectInspectorFactory.getStandardStructObjectInspector(subnames, ois);
			break;
		}
		return result;
	}

	public void matchProtoToRow(Message proto, List<Object> row, List<? extends StructField> structFields) throws Exception {
		for (int i = 0; i < structFields.size(); i++) {
			ObjectInspector oi = structFields.get(i).getFieldObjectInspector();
			String colName = structFields.get(i).getFieldName();
			matchProtoToRowField(proto, row, oi, colName);
		}
	}

	private void matchProtoToRowField(Message m, List<Object> row, ObjectInspector oi, String colName) throws Exception {
		switch (oi.getCategory()) {
		case PRIMITIVE:
			if (this.reflectHas(m, colName)) {
				row.add(reflectGet(m, colName));
			} else {
				row.add(null);
			}
			break;
		case LIST:
			/*
			 * there is no hasList in proto a null list is an empty list. no
			 * need to call hasX
			 */
			Object listObject = reflectGet(m, colName);

			ListObjectInspector li = (ListObjectInspector) oi;
			ObjectInspector subOi = li.getListElementObjectInspector();
			if (subOi.getCategory() == Category.PRIMITIVE) {
				row.add(listObject);
			} else if (subOi.getCategory() == Category.STRUCT) {
				List<Message> x = (List<Message>) listObject;
				StructObjectInspector soi = (StructObjectInspector) subOi;
				List<? extends StructField> substructs = soi.getAllStructFieldRefs();
				List<List<?>> arrayOfStruct = new ArrayList<List<?>>(x.size());
				for (int it = 0; it < x.size(); it++) {
					List<Object> subList = new ArrayList<Object>(substructs.size());
					matchProtoToRow(x.get(it), subList, substructs);
					arrayOfStruct.add(subList);
				}
				row.add(index, arrayOfStruct);
			} else {
				// never should happen
			}
			break;
		case STRUCT:
			if (this.reflectHas(m, colName)) {
				Message subObject = (Message) reflectGet(m, colName);
				StructObjectInspector so = (StructObjectInspector) oi;
				List<? extends StructField> substructs = so.getAllStructFieldRefs();
				List<Object> subList = new ArrayList<Object>(substructs.size());
				matchProtoToRow(subObject, subList, substructs);
				row.add(subList);
			} else {
				row.add(null);
			}
			break;
		}
	}

	public Object reflectGet(Object o, String prop) throws Exception {
		final Class<?> cls = o.getClass();
		final Map<String, Method> classMap = getCachedSubMap(this.cached, cls);
		Method m = classMap.get(prop);
		if (m == null) {
			m = findMethodIgnoreCase(cls, "get" + prop);
			classMap.put(prop, m);
		}
		Object result = m.invoke(o, noArgs);
		return result;
	}

	public Map<String, Method> getCachedSubMap(Map<Class<?>, Map<String, Method>> cache, Class<?> cls) {
		Map<String, Method> classMap = cache.get(cls);
		if (classMap == null) {
			classMap = new HashMap<String, Method>();
			cache.put(cls, classMap);
		}
		return classMap;
	}

	public boolean reflectHas(Object o, String prop) throws Exception {
		final Class<?> cls = o.getClass();
		final Map<String, Method> classMap = getCachedSubMap(this.cachedHas, cls);
		Method m = classMap.get(prop);
		if (m == null) {
			m = findMethodIgnoreCase(cls, "has" + prop);
			classMap.put(prop, m);
		}
		if (m == NO_SUCH_METHOD) {
			return true;
		} else {
			Object result = m.invoke(o, noArgs);
			return (Boolean.TRUE.equals(result));
		}
	}

	public Method findMethodIgnoreCase(Class<?> cls, String methodName) {
		Method[] methods = cls.getMethods();
		for (int i = 0; i < methods.length; i++) {
			if (methods[i].getName().equalsIgnoreCase(methodName)) {
				return methods[i];
			}
		}
		return NO_SUCH_METHOD;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return rowOI;
	}

	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}

}