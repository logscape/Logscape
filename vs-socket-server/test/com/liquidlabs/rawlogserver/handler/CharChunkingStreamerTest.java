package com.liquidlabs.rawlogserver.handler;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CharChunkingStreamerTest {
	List<String> lines = new ArrayList<String>();
	

	@Test
	public void shouldChunkRAWTags() throws Exception {
		StreamHandler myHandler = new StreamHandler() {
			public void handled(byte[] payload, String address, String host, String rootDir) {
				System.out.println("Got:[" + new String(payload) + "]");
				lines.add(new String(payload));
			}
			public void setTimeStampingEnabled(boolean b) {
			}
			public void start() {
			}
			public void stop() {
			}
			public StreamHandler copy() {
				return null;
			}
		};
		CharChunkingHandler streamer = new CharChunkingHandler(myHandler );
		streamer.setCHAR("_raw".toCharArray());
		
		streamer.handled("one_rawtwo_rawhree_raw".getBytes(), "addr", "host", ".");
		Assert.assertEquals(3, lines.size());
		streamer.handled("four_rawfive_rawsi".getBytes(), "addr", "host", ".");
		Assert.assertEquals(5, lines.size());
		streamer.handled("x_raw".getBytes(), "addr", "host", ".");
		Assert.assertEquals(6, lines.size());
		System.out.println("Lines:" + lines);
		
	}
	
	@Test
	public void shouldPutCorrectNewLinesThrough() throws Exception {
		StreamHandler myHandler = new StreamHandler() {
			public void handled(byte[] payload, String address, String host, String rootDir) {
				System.out.println("Got:[" + new String(payload) + "]");
				lines.add(new String(payload));
			}
			public void setTimeStampingEnabled(boolean b) {
			}
			public void start() {
			}
			public void stop() {
			}
			public StreamHandler copy() {
				return null;
			}
		};
		CharChunkingHandler streamer = new CharChunkingHandler(myHandler );
		streamer.handled("one\ntwo\nthree\n".getBytes(), "addr", "host", ".");
		Assert.assertEquals(3, lines.size());
		streamer.handled("four\nfive\nsi".getBytes(), "addr", "host", ".");
		Assert.assertEquals(5, lines.size());
		streamer.handled("x\n".getBytes(), "addr", "host", ".");
		Assert.assertEquals(6, lines.size());
		System.out.println("Lines:" + lines);
	}
}
