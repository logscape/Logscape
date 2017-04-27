package com.liquidlabs.vso.deployment.bundle;

import java.io.File;
import java.io.FileOutputStream;

import static org.junit.Assert.*;

import com.liquidlabs.vso.VSOProperties;
import org.junit.Before;
import org.junit.Test;

public class BundleUnpackerTest {
	
	private BundleUnpacker unpacker;

	@Before
	public void setUp() throws Exception {
		new File("build/sys").mkdirs();
		new File("build/dep").mkdirs();
		new File("build/sys/myBundle-1.0").mkdir();
		new File("build/dep/myBundle-1.0").mkdir();
		unpacker = new BundleUnpacker(new File("build/sys"), new File("build/dep"));
	
	}
	
	@Test
	public void shouldUnpackOverridenBundle() throws Exception {
		VSOProperties.setDownloadsDir("test-data/unpackerTest");
		Bundle unpack = unpacker.unpack(new File("test-data/unpackerTest/XDemoApp-1.0.zip"), true);
		VSOProperties.setDownloadsDir("downloads");
		String selection = unpack.services.get(0).getResourceSelection();
		assertEquals("mflops > 0" , selection);
		
	}
	
	@Test
	public void testShouldUnpackBundleWithCorrectBundleId() throws Exception {
		Bundle unpack = unpacker.unpack(new File("test-data/matrix-DEV-1.0.zip"), true);
		assertNotNull(unpack);
		assertEquals("matrix-DEV-1.0",unpack.getId());
	}
	@Test
	public void testShouldUnpackBundle() throws Exception {
		Bundle unpack = unpacker.unpack(new File("test-data/matrix-1.0.zip"), true);
		assertNotNull(unpack);
	}
	
	@Test
	public void testShouldDeleteSystemBundle() throws Exception {
		Bundle bundle = new Bundle("myBundle", "1.0");
		BundleSerializer serializer = new BundleSerializer(new File(VSOProperties.getDownloadsDir()));
		String xml = serializer.getXML(bundle);
		FileOutputStream fos = new FileOutputStream(new File("build/sys/myBundle-1.0", "myBundle.bundle"));
		fos.write(xml.getBytes());
		fos.close();
		
		boolean wasDeleted = unpacker.deleteExpandedBundleDir("myBundle-1.0");
		
		assertTrue(wasDeleted);
		
	}
	@Test
	public void testShouldGetSystemBundle() throws Exception {
		Bundle bundle = new Bundle("myBundle", "1.0");
		BundleSerializer serializer = new BundleSerializer(new File(VSOProperties.getDownloadsDir()));
		String xml = serializer.getXML(bundle);
		FileOutputStream fos = new FileOutputStream(new File("build/sys/myBundle-1.0", "myBundle.bundle"));
		fos.write(xml.getBytes());
		fos.close();
		
		Bundle bundle2 = unpacker.getBundle("myBundle-1.0");
		
		assertNotNull(bundle2);
		
	}
	@Test
	public void testShouldGetAppBundle() throws Exception {
		Bundle bundle = new Bundle("myBundle", "1.0");
		BundleSerializer serializer = new BundleSerializer(new File(VSOProperties.getDownloadsDir()));
		String xml = serializer.getXML(bundle);
		FileOutputStream fos = new FileOutputStream(new File("build/dep/myBundle-1.0", "myBundle.bundle"));
		fos.write(xml.getBytes());
		fos.close();
		
		Bundle bundle2 = unpacker.getBundle("myBundle-1.0");
		
		assertNotNull(bundle2);
	}

}
