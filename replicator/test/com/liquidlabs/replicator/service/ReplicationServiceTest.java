package com.liquidlabs.replicator.service;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;
import org.joda.time.DateTime;

import com.liquidlabs.common.HashGenerator;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.replicator.ReplicatorProperties;
import com.liquidlabs.replicator.data.Meta;
import com.liquidlabs.replicator.data.Upload;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;

public class ReplicationServiceTest extends MockObjectTestCase {
	private Mock lookupSpace;
	private ReplicationServiceImpl replicationService;
	private ORMapperFactory factory;
	boolean silent = false;
	String hostname = "localhost";
	private boolean forceUpdate;

	protected void setUp() throws Exception {
		lookupSpace = mock(LookupSpace.class);
		lookupSpace.stubs();
		lookupSpace.stubs().method("registerService").withAnyArguments().will(returnValue("xxxx"));
		lookupSpace.stubs().method("unregisterService").withAnyArguments().will(returnValue(true));
		 ReplicatorProperties.setPauseSecsBetweenDeleteAndDownload(1);
		factory = new ORMapperFactory();
		replicationService = new ReplicationServiceImpl(new SpaceServiceImpl((LookupSpace)lookupSpace.proxy(),factory, ReplicationService.NAME, factory.getProxyFactory().getScheduler(), false, false, true));
		replicationService.start();
	}
	
	protected void tearDown() throws Exception {
		replicationService.stop();
	}
	public void testShouldRemoveFile() throws Exception {
		String name = getName();
		String filename = "build" + File.separator + name;
		FileOutputStream fos = new FileOutputStream(filename);
		fos.write("stufffffffffffffff".getBytes());
		fos.close();
		String hash = new HashGenerator().createHash(name, new File(filename));
		
		MyFileListener listener = new MyFileListener();
		replicationService.registerNewFileListener(listener, listener.getId(), -1);
		
		replicationService.publishAvailability(new Meta(name, "build", hash, "localhost",1000, true),-1);
		System.out.println(new DateTime() + " Fileuploaded..........\n\n\n");
		replicationService.fileUploaded(new Upload(hash, name, "build", 1), hostname, forceUpdate);
		ReplicatorProperties.setNewFileWaitSecs(1);
		Thread.sleep(1500);
		replicationService.remove("test", new Upload(hash, name, "build", 1));
		
		Thread.sleep(500);
		assertEquals(1, listener.remove);
	}
	public void testShouldRemoveFileNameWithDots() throws Exception {
		String name =  "..." + getName() + ".log";
		String filename = "build" + File.separator + name;
		FileOutputStream fos = new FileOutputStream(filename);
		fos.write("stufffffffffffffff".getBytes());
		fos.close();
		String hash = new HashGenerator().createHash(name, new File(filename));
		
		MyFileListener listener = new MyFileListener();
		replicationService.registerNewFileListener(listener, listener.getId(), -1);
		
		replicationService.publishAvailability(new Meta(name, "build", hash, "localhost",1000, true),-1);
		replicationService.fileUploaded(new Upload(hash, name, "build", 1), hostname, forceUpdate);
		ReplicatorProperties.setNewFileWaitSecs(1);
		Thread.sleep(1500);
		replicationService.remove("test", new Upload(hash, name, "build", 1));
		
		Thread.sleep(100);
		assertEquals(1, listener.remove);
		
	}

	
	public void testShouldRetrieveMetaList() throws Exception {
		String hash = "76767676";
		Meta meta = new Meta("file", "path", hash, "myhost", 80, true);
		Meta meta2 = new Meta("file", "path", hash, "myhost-1", 80, true);
		replicationService.publishAvailability(meta, 360);
		replicationService.publishAvailability(meta2, 360);
		List<Meta> retrieveCurrentForHash = replicationService.retrieveCurrentForHash(hash, "path", "file");
		assertEquals(2, retrieveCurrentForHash.size());
	}
	
	public void testShouldGetHashesOfUploadedFiles() throws Exception {
		replicationService.fileUploaded(new Upload("hash1", "file1", "path", 1), hostname, forceUpdate);
		replicationService.fileUploaded(new Upload("hash2", "file2", "path", 1), hostname, forceUpdate);
		replicationService.fileUploaded(new Upload("hash3", "file3", "path", 1), hostname, forceUpdate);
		replicationService.fileUploaded(new Upload("hash4", "file4", "path", 1), hostname, forceUpdate);
		Thread.sleep(5500);
		
		List<Upload> deployedFiles = replicationService.getDeployedFiles();
		assertEquals(4, deployedFiles.size());
	}
	
	static class MyFileListener implements NewFileListener {
		public int remove = 0;
		public int add = 0;
		
		public String getId() {
			return "test-listener";
		}

		public void newFileUploaded(String fileName, String path, String hash, int pieces) {
			System.out.println(new DateTime() + " Added");
			add++;
		}

		public void removeFile(String hash, String fileName, String path, boolean forceRemove) {
			System.out.println("Removed");
			remove++;
		}
	}
	
	public void testShouldRemoveOldFilesWithSameName() throws Exception {
		// make new file wait seconds low - so the remove event gets executed on the listener
		ReplicatorProperties.setNewFileWaitSecs(1);
		
		MyFileListener listener = new MyFileListener();
		replicationService.registerNewFileListener(listener, listener.getId(), -1);
		replicationService.fileUploaded(new Upload("hash1", "myFile", "path", 1), hostname, forceUpdate);
		Thread.sleep(5500);
		// this update will trigger a removal of the old file data
		replicationService.fileUploaded(new Upload("hash2", "myFile", "path", 1), hostname, forceUpdate);
		Thread.sleep(5500);
		assertEquals(1, replicationService.getDeployedFiles().size());
		assertEquals(2, listener.add);
		assertEquals(1, listener.remove);
	}
}
