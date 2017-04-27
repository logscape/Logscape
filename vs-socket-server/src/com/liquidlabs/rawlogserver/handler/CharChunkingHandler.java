package com.liquidlabs.rawlogserver.handler;

import com.liquidlabs.common.collection.Arrays;

import java.util.List;

/**
 * Waits until a full line event is received before passing on data - line by line
 * @author neil
 */
public class CharChunkingHandler implements StreamHandler {
	
	char[] chars = new char[] { '\n' };
	private final StreamHandler next;
	boolean splitAtStart = false;

	public CharChunkingHandler(StreamHandler nextHandler) {
		this.next = nextHandler;
	}
	public void setCHAR(char[] char1) {
		chars = char1;
	}
	
	String lastResults = "";

	public void handled(final byte[] payload, final String address, final String host, final String rootDir) {
		String payloadString = lastResults + new String(payload);
		byte[] allBytes = payloadString.getBytes();
		List<Integer> offsets = Arrays.getSplits(chars, payloadString.toCharArray(), true, splitAtStart);
		if (offsets.size() == 0) {
			lastResults = payloadString;
			return;
		}
		if (offsets.size() > 0 && offsets.get(0) != 0) {
			offsets.add(0, 0);
		}
		for (int i = 0; i < offsets.size()-1; i++) {
			Integer from = offsets.get(i);
			Integer to = offsets.get(i+1);
			next.handled(java.util.Arrays.copyOfRange(allBytes, from.intValue(), to), address, host, rootDir);
		}
		if (offsets.size() == 1) {
			lastResults = payloadString;
		} else {
			lastResults = payloadString.substring(offsets.get(offsets.size()-1));
		}
		
	}
	public StreamHandler copy() {
		return new CharChunkingHandler(next.copy());
	}

	public void setTimeStampingEnabled(boolean b) {
		next.setTimeStampingEnabled(b);
	}

	public void start() {
		next.start();
	}

	public void stop() {
		next.stop();
	}
	public void setSplitAtStart(boolean splitAtStart) {
		this.splitAtStart = splitAtStart;
	}
}
