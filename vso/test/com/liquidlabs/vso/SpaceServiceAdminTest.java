package com.liquidlabs.vso;

import junit.framework.TestCase;

public class SpaceServiceAdminTest extends TestCase {
	
	
	public void testDumpThreads() throws Exception {
		
		SpaceServiceAdmin admin = new SpaceServiceAdmin("stuff", null, null);
		String dumpThread = admin.threadDump("stuff");
		assertNotNull(dumpThread);
		
	}

}
