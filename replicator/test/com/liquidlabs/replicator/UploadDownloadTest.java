package com.liquidlabs.replicator;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import com.liquidlabs.common.HashGenerator;
import com.liquidlabs.replicator.data.MetaInfo;
import com.liquidlabs.replicator.download.DownloadListener;
import com.liquidlabs.replicator.download.DownloadManager;
import com.liquidlabs.replicator.service.UploadService;
import com.liquidlabs.space.lease.LeaseRenewalService;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.agent.ResourceAgent;

public class UploadDownloadTest extends TestCase {
	
	private File file;
	private ExecutorService executor;
	private FakeReplicationService replicator;
	
	private ProxyFactoryImpl proxyFactoryDownloadService;
	HashGenerator hasher = new HashGenerator();
	
	@Override
	protected void setUp() throws Exception {
	
		
		executor = Executors.newCachedThreadPool();
		
		
		replicator = new FakeReplicationService();
		
		TransportFactory transportFactory2 = new TransportFactoryImpl(executor, "test");
		proxyFactoryDownloadService = new ProxyFactoryImpl(transportFactory2,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 22222, "serviceName"), executor, "");
		proxyFactoryDownloadService.start();
		
		
		Thread.sleep(1000);
		System.out.println("Starting:" + getName());
		
		
	}
	@Override
	protected void tearDown() throws Exception {
		proxyFactoryDownloadService.stop();
	}
	

	class MyDownloadListener implements DownloadListener {
		public void downloadComplete(File tempFile, String hash) {
		}

		public void downloadRemoved(File name) {
		}
	}
	
	public void testShouldDownloadStuffFromMultipleSources() throws Exception {
		
		TransportFactory transportFactoryONE = new TransportFactoryImpl(executor, "test");
		ProxyFactoryImpl proxyFactoryUploadServiceONE = new ProxyFactoryImpl(transportFactoryONE,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 11111, "serviceName"), executor, "");
		
		UploadManager uploadManagerONE = uploadServiceONE(proxyFactoryUploadServiceONE);
		
		
		TransportFactory transportFactoryTWO = new TransportFactoryImpl(executor, "test");
		ProxyFactoryImpl proxyFactoryUploadServiceTWO = new ProxyFactoryImpl(transportFactoryTWO,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 11111, "serviceName"), executor, "");
		
		UploadManager uploadManagerTWO = uploadServiceONE(proxyFactoryUploadServiceTWO);

		
		file = File.createTempFile("this", "def");
		System.out.println("---------------- SourceFile:" + file.getAbsolutePath());
		file.deleteOnExit();
		new File("build").mkdirs();
		writeBytes(1000 * 1024);
		
		MetaInfo metaInfo = new MetaInfo(file, 10);
		uploadManagerONE.publish(metaInfo);
		uploadManagerTWO.publish(metaInfo);
		
		
		System.out.println("Started");
		Thread.sleep(1000);
		DownloadManager downloadManager = new DownloadManager(replicator, new FakeLeaseRenewer(), proxyFactoryDownloadService, new SysBouncer() {
			
			public void bounce(ResourceAgent agent) {
			}}, "resourceId");
		Thread.sleep(500);
		
		File download = downloadManager.download(file.getName(),"build", metaInfo.hash(), metaInfo.pieceCount(), new MyDownloadListener(), 1);
		
		proxyFactoryUploadServiceONE.stop();
		
		assertNotNull(download);
		
		
		assertEquals(hasher.createHash(file.getName(), file), hasher.createHash(download.getName(), download));
	}
	
	public void testShouldDownloadStuffFromSingleSource() throws Exception {
		
		TransportFactory transportFactory = new TransportFactoryImpl(executor, "test");
		ProxyFactoryImpl proxyFactoryUploadService = new ProxyFactoryImpl(transportFactory,  TransportFactoryImpl.getDefaultProtocolURI("", "localhost", 11111, "serviceName"), executor, "");
		
		UploadManager uploadManagerONE = uploadServiceONE(proxyFactoryUploadService);

		
		file = File.createTempFile("this", "def");
		file.deleteOnExit();
		new File("build").mkdirs();
		writeBytes(1000 * 1024);
		
		MetaInfo metaInfo = new MetaInfo(file, 10);
		uploadManagerONE.publish(metaInfo);
		
		
		System.out.println("Started");
		Thread.sleep(1000);
		DownloadManager downloadManager = new DownloadManager(replicator, new FakeLeaseRenewer(), proxyFactoryDownloadService, new SysBouncer() {
			
			public void bounce(ResourceAgent agent) {
			}}, "resourceId");
		Thread.sleep(500);
		
		File download = downloadManager.download(file.getName(),"build", metaInfo.hash(), metaInfo.pieceCount(), new MyDownloadListener(), 1);
		
		proxyFactoryUploadService.stop();
		
		assertNotNull(download);
		
		
		assertEquals(hasher.createHash(file.getName(), file), hasher.createHash(download.getName(), download));
	}
	private UploadManager uploadServiceONE(ProxyFactoryImpl proxyFactoryUploadService) {
		UploadManager uploadManager = new UploadManager(replicator, new FakeLeaseRenewer(), proxyFactoryUploadService.getAddress().getPort());
		LeaseRenewalService leaseRenewalService = new LeaseRenewalService(null, Executors.newScheduledThreadPool(5));
		UploadService uploadService = new UploadService(uploadManager, replicator, proxyFactoryUploadService, leaseRenewalService, null, "save", null, "resourceId");
		proxyFactoryUploadService.registerMethodReceiver(UploadService.NAME, uploadService);
		proxyFactoryUploadService.start();
		return uploadManager;
	}
	
	
	private void writeBytes(int kbytes) throws FileNotFoundException,IOException {
		FileOutputStream stream = new FileOutputStream(file);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int written = 0;
		byte b = "a".getBytes()[0];
		while (written < kbytes) {
			byte[] buf = new byte[1024 * 5];
			Arrays.fill(buf, b);
			baos.write(buf);
			b++;
			written += buf.length;
		}
		stream.write(baos.toByteArray());
		stream.close();
	}
}
