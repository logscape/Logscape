package com.liquidlabs.transport.proxy.events;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.transport.Config;

public class Event {
	private static final String _NW_AND_ = "_NW_AND_";
	public enum Type { READ, WRITE, UPDATE, TAKE;

	public static Event.Type[] fromString(String value) {
		String[] split2 = Arrays.split("*", value);
		Event.Type[] results = new Event.Type[split2.length];
		int pos = 0;
		for (String string : split2) {
			results[pos++] = Event.Type.valueOf(string);
		}
		return results;
	} }
	
	public static String KEY = "_EVENT_";
	public static String SPLIT = Config.NETWORK_SPLIT;
	
	String id;
	Type type;
	String key;
	private String value;
	private String source;
	
	public Event(){
	}
	public Event(String key, String value, Type eventType){
		this.type = eventType;
		this.key = key;
		this.value = value ==  null ? null : value.replaceAll(SPLIT, _NW_AND_);
	}
	public Event(String id, String key, String value, Type eventType){
		this.id = id;
		this.type = eventType;
		this.key = key;
		this.value = value ==  null ? null : value.replaceAll(SPLIT, _NW_AND_);
	}

	public Type getType() {
		return type;
	}
	public void setType(Type type) {
		this.type = type;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getSource() {
		return source;
	}
	public String getValue() {
		if (value == null) return null;
		return value.replaceAll(_NW_AND_, SPLIT);
	}
	public void setValue(String value) {
		this.value = value.replaceAll(SPLIT, _NW_AND_);
	}
	public String toString(){
		return Arrays.appendWithDelim(SPLIT, KEY, id, key, value, type.name(), source);
	}
	public void fromString(String payload){
		String[] split2 = Arrays.split(SPLIT, payload);
		fromStringArray(split2);
	}
	public void fromStringArray(Object[] split2) {
		this.id = split2[1].toString();
		this.key = split2[2].toString();
		this.value = split2[3].toString();
		String string = split2[4].toString();
		try {
			this.type = Type.valueOf(string);
		} catch (Throwable t){
			throw new RuntimeException(String.format("Failed to handleEvent eventType for String[%s]", string));
		}
		this.source = split2[5].toString();
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public static boolean isA(String stringValue) {
		return stringValue != null && stringValue.startsWith(KEY);
	}
	public String getSourceURI() {
		if (source == null) return null;
		return source.replaceAll(_NW_AND_, SPLIT);
	}
	public void setSource(String source) {
		this.source = source.replaceAll(SPLIT, _NW_AND_);;
	}
}
