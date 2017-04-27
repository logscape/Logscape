package com.liquidlabs.rawlogserver.handler;

import javolution.util.FastMap;
import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * Clones pipelines for each address that is added.
 * @author neil
 *
 */
public class PerAddressHandler implements StreamHandler {
    private final static Logger LOGGER = Logger.getLogger(PerAddressHandler.class);
	
	private final StreamHandler next;
	FastMap<String, StreamHandler> handlers = new FastMap<String, StreamHandler>();

	public PerAddressHandler(StreamHandler next) {
		handlers.shared();
		this.next = next;
	}

	public void handled(byte[] payload, String address, String host, String rootDir) {
		StreamHandler next = handlers.get(address);
		if (next == null) { 
			next = this.next.copy();
			next.start();
			handlers.put(address, next);
            LOGGER.info("Client:Socket Action:ConnectionEstablished Address:" + address);
		}
		next.handled(payload, address, host, rootDir);
	}

	public void setTimeStampingEnabled(boolean b) {
	}
	public StreamHandler copy() {
		throw new RuntimeException("Not implemented");
	}

	public void start() {
	}

	public void stop() {
		Collection<StreamHandler> values = handlers.values();
		for (StreamHandler streamHandler : values) {
			try {
				streamHandler.stop();
			} catch (Exception e) {
			}
		}
	}

}
