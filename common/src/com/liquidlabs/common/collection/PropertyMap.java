package com.liquidlabs.common.collection;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class PropertyMap implements Map<String, String> {
	
	private final static Logger LOGGER = Logger.getLogger(PropertyMap.class);
	private Map<String, String> properties;
	
	public PropertyMap(String propertiesString) {
		putString(propertiesString);
	}

	public String toString(){
		StringBuilder stringBuilder = new StringBuilder();
		Set<String> keySet = properties.keySet();
		for (String key : keySet) {
			stringBuilder.append(key).append("=").append(properties.get(key)).append(",");
		}
		return stringBuilder.toString();
	}
	
	public void put(String propertyString) {
		putString(this.toString() + "," + propertyString);
	}
	
	private void putString(String propertiesString) {
		if (propertiesString == null) propertiesString = "";
		String[] splitProperties = Arrays.split(",", propertiesString);
		HashMap<String, String> propertyMap = new HashMap<String, String>();
		for (String propertyString : splitProperties) {
			String[] split = Arrays.split("=", propertyString);
			if (split.length != 2){
//				LOGGER.info("Ignoring - Unexpected PropertyStringLength, length:" + split.length + " split[]:"+ java.util.Arrays.toString(split) + " from:" + propertyString + " source:" + propertiesString);
				continue;
				
			}
			propertyMap.put(split[0].trim(), split[1].trim());
		}
		this.properties = propertyMap;
	}


	public void clear() {
		properties.clear();
	}
	public boolean containsKey(Object key) {
		return properties.containsKey(key);
	}
	public boolean containsValue(Object value) {
		return properties.containsValue(value);
	}
	public Set<Entry<String, String>> entrySet() {
		return properties.entrySet();
	}
	public boolean equals(Object o) {
		return properties.equals(o);
	}
	public String get(Object key) {
		return properties.get(key);
	}
	public int hashCode() {
		return properties.hashCode();
	}
	public boolean isEmpty() {
		return properties.isEmpty();
	}
	public Set<String> keySet() {
		return properties.keySet();
	}
	public String put(String key, String value) {
		return properties.put(key, value);
	}
	public boolean putAllWithNewValueAddedResult(Map<? extends String, ? extends String> m) {
		boolean newValueAdded = false;
		Set<? extends String> keySet = m.keySet();
		for (String newKey : keySet) {
			if (!this.properties.containsKey(newKey)){
				newValueAdded = true;
			}
		}
		properties.putAll(m);
		return newValueAdded;
	}
	public void putAll(Map<? extends String, ? extends String> m) {
		properties.putAll(m);
	}
	public String remove(Object key) {
		return properties.remove(key);
	}
	public int size() {
		return properties.size();
	}
	public Collection<String> values() {
		return properties.values();
	}

}
