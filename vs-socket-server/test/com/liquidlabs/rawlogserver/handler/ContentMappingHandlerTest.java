package com.liquidlabs.rawlogserver.handler;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ContentMappingHandlerTest {
	
	private ContentMappingHandler handler;


	@Before
	public void before() {
		handler = new ContentMappingHandler();
	}
	int calledOne = 0;
	int calledTwo = 0;
	@Test
	public void shouldMapASplunkStream() throws Exception {
		
		StreamHandler splunkOne = new StreamHandler() {
			
			public StreamHandler copy() {
				return null;
			}
			public void handled(byte[] payload, String address, String host, String rootDir) {
				calledOne++;
			}
			public void setTimeStampingEnabled(boolean b) {
			}
			public void start() {
			}
			public void stop() {
			}
		};
		StreamHandler defaultOne = new StreamHandler() {
			public StreamHandler copy() {
				return null;
			}
			public void handled(byte[] payload, String address, String host, String rootDir) {
				calledTwo++;
			}
			public void setTimeStampingEnabled(boolean b) {
			}
			public void start() {
			}
			public void stop() {
			}
		};
		handler.addHandler("_raw", splunkOne);
		handler.addDefaultHandler(defaultOne);
		String payload = "_raw E2011 May 09 18:17:53 (CLONWWMAIL1) 192.168.3.100->WinEvtLog\r\n"; 
		handler.handled(payload.getBytes(), "addr", "host", "build");
		assertEquals(1, calledOne);
	}

}
