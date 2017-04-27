package com.liquidlabs.vso.deployment.bundle;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;

public class BundleTest {
	
	@Test
	public void shouldMatchFile() throws Exception {
		String id = new Bundle().getBundleIdForFilename(new File("/opt/deployed-bundles/SomeThing-1.0/file.bundle").getParentFile().getName());
		assertEquals("SomeThing-1.0", id);
		
	}

}
