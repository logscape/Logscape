package com.liquidlabs.rawlogserver.handler;

import com.liquidlabs.common.concurrent.NamingThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TeeingHandler implements StreamHandler {

	private final StreamHandler next;
	ExecutorService teePool = Executors.newSingleThreadExecutor(new NamingThreadFactory("TeePool"));
	private final StreamHandler teeHandler;

	public StreamHandler copy() {
		return new TeeingHandler(teeHandler.copy(), next.copy());
	}
	
	public TeeingHandler(StreamHandler teeHandler, StreamHandler next) {
		this.teeHandler = teeHandler;
		this.next = next;
	}

	public void handled(final byte[] payload, final String address, final String host, final String rootDir) {
//		teePool.execute(new Runnable() {
//			public void run() {
		try {
				teeHandler.handled(payload, address, host, rootDir);
		} catch (Throwable t) {
			t.printStackTrace();
		}
				
		try {
//			}
//		});
			next.handled(payload, address, host, rootDir);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	public void setTimeStampingEnabled(boolean b) {
	}

	public void start() {
		teeHandler.start();
		next.start();
	}

	public void stop() {
		teeHandler.stop();
		next.stop();
	}
}
