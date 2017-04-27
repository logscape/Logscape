package com.liquidlabs.rawlogserver.handler;

import com.liquidlabs.common.collection.Arrays;
import javolution.util.FastMap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * maps a byte[] to be processed by a matching handlers
 */
public class ContentMappingHandler implements StreamHandler {
	
	Map<String, StreamHandler> handlers = new FastMap<String, StreamHandler>();
	StreamHandler defaultHandler;
	
	public void addHandler(String key, StreamHandler handler) {
		handlers.put(key, handler);
	}
	public void addDefaultHandler(StreamHandler handler) {
		this.defaultHandler = handler;
	}

	public StreamHandler copy() {
		throw new RuntimeException("Not Implemented");
	}

	public void handled(byte[] payload, String address, String host, String rootDir) {
		Set<String> keySet = handlers.keySet();
		for (String key : keySet) {
			if (Arrays.arrayContains(key.getBytes(), payload) != -1) {
				handlers.get(key).handled(payload, address, host, rootDir);
				return;
			}
		}
		String str = new String(payload);
		defaultHandler.handled(payload, address, host, rootDir);
	}

	
	public void setTimeStampingEnabled(boolean b) {
	}

	public void start() {
		defaultHandler.start();
		Collection<StreamHandler> values = this.handlers.values();
		for (StreamHandler streamHandler : values) {
			streamHandler.start();
		}
	}

	public void stop() {
		defaultHandler.stop();
		Collection<StreamHandler> values = this.handlers.values();
		for (StreamHandler streamHandler : values) {
			streamHandler.stop();
		}
		
	}

}
