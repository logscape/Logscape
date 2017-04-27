/**
 * 
 */
package com.liquidlabs.common.collection;

import java.util.concurrent.ConcurrentHashMap;

import javolution.util.FastMap;

import com.thoughtworks.xstream.converters.collections.MapConverter;
import com.thoughtworks.xstream.mapper.Mapper;

/**
 * XStream convertor to support nice xml data
 * @author neil
 *
 */
public class LLMapConvertor extends MapConverter {

	public LLMapConvertor(Mapper mapper) {
		super(mapper);
	}
	public boolean canConvert(Class type) {
		if (type.equals(ConcurrentHashMap.class) || type.equals(FastMap.class)) {
			return true;
		}
		return super.canConvert(type);
	}
}