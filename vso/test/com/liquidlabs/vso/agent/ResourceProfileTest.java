package com.liquidlabs.vso.agent;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.joda.time.DateTime;
import org.junit.Test;

import com.liquidlabs.common.HashGenerator;
import com.liquidlabs.transport.serialization.ObjectTranslator;

public class ResourceProfileTest {
	
	
	@Test
	public void shouldCacheBundlehash() throws Exception {
		ResourceProfile profile = new ResourceProfile();
		String hash1 = profile.getHashForFile(new File("test-data/matrix-1.0.zip"));
		String hash2 = profile.getHashForFile(new File("test-data/matrix-1.0.zip"));
		assertEquals(hash1, hash2);
		
	}
	@Test
	public void shouldGetGoodMemory() throws Exception {
		ResourceProfile profile = new ResourceProfile();
		profile.oneOffUpdate();
		double sysMemUtil = profile.getSysMemUtil();
		double swapUtil = profile.getSwapUtil();
		System.out.println("Util:" + sysMemUtil);
	}
	
	@Test
	public void shouldListWorkIdsReadFriendly() throws Exception {
		ResourceProfile profile = new ResourceProfile();
		profile.oneOffUpdate();
		profile.addWorkAssignmentId("vs-log-1.0:LogTailer");
		profile.addWorkAssignmentId("replicator-1.0:Downloader");
		profile.addWorkAssignmentId("replicator-1.0:Downloader");
		profile.addWorkAssignmentId("vs-log-1.0:LogTailer");
		assertEquals("replicator-1.0:Downloader,\nvs-log-1.0:LogTailer", profile.getWorkId());
		profile.addWorkAssignmentId("replicator-1.0:Uploader");
		assertEquals("replicator-1.0:Downloader,\nreplicator-1.0:Uploader,\nvs-log-1.0:LogTailer", profile.getWorkId());
		
		profile.removeWorkAssignmentId("replicator-1.0:Downloader");
		assertEquals("replicator-1.0:Uploader,\nvs-log-1.0:LogTailer", profile.getWorkId());
	}
	
	@Test
	public void shouldMatchOnDownloadedFile() throws Exception {
		
		ResourceProfile resourceProfile = new ResourceProfile();
		resourceProfile.oneOffUpdate();
		resourceProfile.timeDelta = -12141;
		
		DateTime masterTime = new DateTime();
		
		String contents =  "fglksmdfglsdkfmgsldfkmgsdlfkmgs;dlfkmgsd;lfmg";
		String hash = new HashGenerator().createHash(contents.getBytes());
		
		resourceProfile.downloadHashes = "boot.zip #:" + hash + "\r\n" + 
				"dashboard-1.0.zip #:34523452345234"; 
		
		assertTrue(resourceProfile.isFileDownloaded("boot.zip", masterTime.getMillis(), 840, hash));
	}

	@Test
	public void shouldNotMatchWhenFileNameNotDownloaded() throws Exception {
		ResourceProfile resourceProfile = new ResourceProfile();
		resourceProfile.oneOffUpdate();
		String goodHash = new HashGenerator().createHash("fglksmdfglsdkfmgsldfkmgsdlfkmgs;dlfkmgsd;lfmg".getBytes());
		String badHash = new HashGenerator().createHash("BAD_fglksmdfglsdkfmgsldfkmgsdlfkmgs;dlfkmgsd;lfmg".getBytes());
		
		resourceProfile.downloadHashes = "boot.zip #:" + badHash + "\r\n" + 
				"dashboard-1.0.zip #:34523452345234"; 
		
		assertFalse(resourceProfile.isFileDownloaded("myOtherFileName.zip", 10000, 10000 / 1024, goodHash));
		
	}
	@Test
	public void testLogStats() throws Exception {
		ResourceProfile resourceProfile = new ResourceProfile();
		resourceProfile.oneOffUpdate();
		String logStats = resourceProfile.getSystemStats("TAG", "ID");
		System.out.println("LogStats:" + logStats);
		assertTrue(logStats.length() > 0);
		
	}
	
	@Test
	public void testShouldGetDomain() throws Exception {
		ResourceProfile resourceProfile = new ResourceProfile();
		assertNotNull(resourceProfile.getDomain());
		
	}
		
	@Test
	public void testTofromResourceProfile() throws Exception {
		ResourceProfile resourceProfile = new ResourceProfile();
		resourceProfile.lastUpdatedMs = 0;
		resourceProfile.updateValues();
		
		ObjectTranslator query = new ObjectTranslator();
		String stringFromObject = query.getStringFromObject(resourceProfile);
		
		ResourceProfile newResourceProfile = query.getObjectFromFormat(ResourceProfile.class, stringFromObject);
		assertNotNull(stringFromObject);
		assertNotNull(newResourceProfile);
		System.out.println(stringFromObject);
	}
	
	@Test
	public void testShouldMatchOnResourceIdContains() throws Exception {
		ResourceProfile resourceProfile = new ResourceProfile();
		resourceProfile.setResourceId("myStuff-0");
		ObjectTranslator query = new ObjectTranslator();
		assertTrue("should have been true", query.isMatch("resourceId contains '-0'", resourceProfile));

		
	}
	
	@Test
	public void testShouldCreateSpaceFormat() throws Exception {
		
		ResourceProfile resourceProfile = new ResourceProfile();
		resourceProfile.setPort(11001);
		ObjectTranslator query = new ObjectTranslator();
		String stringFromObject = query.getStringFromObject(resourceProfile);
		assertNotNull(stringFromObject);
		System.out.println(query.getAsCSV(resourceProfile));
	}
	
	@Test
	public void testUpdateDoesntBlowup() throws Exception {
		ResourceProfile resourceProfile = new ResourceProfile();
		resourceProfile.updateValues();
		
		ObjectTranslator query = new ObjectTranslator();
		String stringFromObject = query.getStringFromObject(resourceProfile);
		assertNotNull(stringFromObject);
		System.out.println(stringFromObject);
	}

	@Test
	public void testShouldNotMatchOnQuery() throws Exception {
		ResourceProfile resourceProfile = new ResourceProfile();
		String workId = "mobius.local-11035:myBundle-0.01:ORM";
		resourceProfile.addWorkAssignmentId(workId);
		ObjectTranslator query = new ObjectTranslator();
		assertFalse(String.format("should be false as %s does contain ORM", resourceProfile.getWorkId()),query.isMatch("workId notContains myBundle-0.01:ORM AND workId notContains myBundle-0.01:FOO", resourceProfile));
	}
}
