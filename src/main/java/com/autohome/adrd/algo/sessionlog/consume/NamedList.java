package com.autohome.adrd.algo.sessionlog.consume;


import java.util.*;
import java.io.Serializable;
/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */
public class NamedList<T> implements Cloneable, Serializable, Iterable<Map.Entry<String, T>> {
	
	protected final List nvPairs;

	public NamedList() {
		nvPairs = new ArrayList();
	}

	public NamedList(Map.Entry<String, ? extends T>[] nameValuePairs) {
		nvPairs = nameValueMapToList(nameValuePairs);
	}

	public NamedList(List nameValuePairs) {
		nvPairs = nameValuePairs;
	}

	private List nameValueMapToList(Map.Entry<String, ? extends T>[] nameValuePairs) {
		List result = new ArrayList();
		for (Map.Entry<String, ?> ent : nameValuePairs) {
			result.add(ent.getKey());
			result.add(ent.getValue());
		}
		return result;
	}
	
	public void clear() {
		if (nvPairs != null) nvPairs.clear();
	}

	public int size() {
		return nvPairs.size() >> 1;
	}

	public String getName(int idx) {
		return (String) nvPairs.get(idx << 1);
	}

	public T getVal(int idx) {
		return (T) nvPairs.get((idx << 1) + 1);
	}

	public void add(String name, T val) {
		nvPairs.add(name);
		nvPairs.add(val);
	}

	public void setName(int idx, String name) {
		nvPairs.set(idx << 1, name);
	}

	public T setVal(int idx, T val) {
		int index = (idx << 1) + 1;
		T old = (T) nvPairs.get(index);
		nvPairs.set(index, val);
		return old;
	}

	public T remove(int idx) {
		int index = (idx << 1);
		nvPairs.remove(index);
		return (T) nvPairs.remove(index);
	}

	public int indexOf(String name, int start) {
		int sz = size();
		for (int i = start; i < sz; i++) {
			String n = getName(i);
			if (name == null) {
				if (n == null) return i;
			} else if (name.equals(n)) {
				return i;
			}
		}
		return -1;
	}

	public T get(String name) {
		return get(name, 0);
	}

	public T get(String name, int start) {
		int sz = size();
		for (int i = start; i < sz; i++) {
			String n = getName(i);
			if (name == null) {
				if (n == null) return getVal(i);
			} else if (name.equals(n)) {
				return getVal(i);
			}
		}
		return null;
	}

	public List<T> getAll(String name) {
		List<T> result = new ArrayList<T>();
		int sz = size();
		for (int i = 0; i < sz; i++) {
			String n = getName(i);
			if (name == n || (name != null && name.equals(n))) {
				result.add(getVal(i));
			}
		}
		return result;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('{');
		int sz = size();
		for (int i = 0; i < sz; i++) {
			if (i != 0) sb.append(',');
			sb.append(getName(i));
			sb.append('=');
			sb.append(getVal(i));
		}
		sb.append('}');

		return sb.toString();
	}

	public static final class NamedListEntry<T> implements Map.Entry<String, T> {

		public NamedListEntry() {
		}

		public NamedListEntry(String _key, T _value) {
			key = _key;
			value = _value;
		}

		public String getKey() {
			return key;
		}

		public T getValue() {
			return value;
		}

		public T setValue(T _value) {
			T oldValue = value;
			value = _value;
			return oldValue;
		}

		private String key;

		private T value;
	}

	public boolean addAll(Map<String, T> args) {
		for (Map.Entry<String, T> entry : args.entrySet()) {
			add(entry.getKey(), entry.getValue());
		}
		return args.size() > 0;
	}

	public boolean addAll(NamedList<T> nl) {
		nvPairs.addAll(nl.nvPairs);
		return nl.size() > 0;
	}

	public NamedList<T> clone() {
		ArrayList newList = new ArrayList(nvPairs.size());
		newList.addAll(nvPairs);
		return new NamedList<T>(newList);
	}

	public Iterator<Map.Entry<String, T>> iterator() {

		final NamedList list = this;

		Iterator<Map.Entry<String, T>> iter = new Iterator<Map.Entry<String, T>>() {
			int idx = 0;

			public boolean hasNext() {
				return idx < list.size();
			}

			public Map.Entry<String, T> next() {
				final int index = idx++;
				Map.Entry<String, T> nv = new Map.Entry<String, T>() {
					public String getKey() {
						return list.getName(index);
					}

					@SuppressWarnings("unchecked")
					public T getValue() {
						return (T) list.getVal(index);
					}

					public String toString() {
						return getKey() + "=" + getValue();
					}

					public T setValue(T value) {
						return (T) list.setVal(index, value);
					}
				};
				return nv;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
		return iter;
	}

	public T remove(String name) {
		int idx = indexOf(name, 0);
		if (idx != -1)
			return remove(idx);
		return null;
	}

	public int hashCode() {
		return nvPairs.hashCode();
	}

	public boolean equals(Object obj) {
		if (!(obj instanceof NamedList)) return false;
		NamedList nl = (NamedList) obj;
		return this.nvPairs.equals(nl.nvPairs);
	}
	
}