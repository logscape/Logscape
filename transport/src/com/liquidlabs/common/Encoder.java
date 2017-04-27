package com.liquidlabs.common;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public class Encoder {

	public String encode(Map map) {
		StringBuilder builder = new StringBuilder();
		try {
			encode(builder, map);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Failed to encode map", e);
		}
		return builder.toString();
	}

	private void encode(StringBuilder builder, Map map) throws UnsupportedEncodingException {
		builder.append("d");
		Set<Map.Entry> entrySet = map.entrySet();
		for (Map.Entry entry : entrySet) {
			String key = entry.getKey().toString();
			builder.append(key.length()).append(":").append(key); 
			encode(builder, entry.getValue());
		}
		builder.append("e");
	}
	
	private void encode(StringBuilder builder, Object value) throws UnsupportedEncodingException {
		if (value instanceof String) {
			encode(builder, (String)value);
		} else if (value instanceof List) {
			encode(builder, (List)value);
		} else if (value instanceof Map) {
			encode(builder, (Map)value);
		} else if (value instanceof Number) {
			encode(builder, (Number)value);
		} else if (value instanceof byte[]) {
			encode(builder, (byte[])value);
		}
	}
	
	private void encode(StringBuilder builder, byte[] bytes) throws UnsupportedEncodingException {
		builder.append(bytes.length).append(":").append(new String(bytes, "UTF-8"));
	}
	
	private void encode(StringBuilder builder, Number value) {
		builder.append("i").append(value).append("e");
	}
	
	private void encode(StringBuilder builder, String value) {
		builder.append(value.length()).append(":").append(value);
	}
	
	private void encode(StringBuilder builder, List list) throws UnsupportedEncodingException {
		builder.append("l");
		for (Object object : list) {
			encode(builder, object);
		}
		builder.append("e");
	}
}