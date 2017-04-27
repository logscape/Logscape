package com.liquidlabs.rawlogserver;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.EndPoint;
import com.liquidlabs.transport.netty.NettyEndPointFactory;
import com.liquidlabs.transport.netty.StreamState;
import com.liquidlabs.transport.netty.StringProtocolParser;
import org.jboss.netty.buffer.ChannelBuffer;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

public class RawLogServerIntegrationTest {
	String dir ="/";//c:\\work\\logs\\clone-systems\\raw-socket";
	
	@Test
	public void shouldCreateData() throws Exception {
		String rootDir = "build/RawIntegTestREAL";
		FileUtil.deleteDir(new File(rootDir));
		ExecutorService executor = Executors.newCachedThreadPool();
		
		final RawLogServer server = new RawLogServer(rootDir);
		server.setHostAddressOnly(false);
		server.start();
		URI uri = new URI("raw://localhost:8991");
		NettyEndPointFactory epFactory = new NettyEndPointFactory(Executors.newScheduledThreadPool(3), "test");
		epFactory.start();
		EndPoint endPoint = epFactory.getEndPoint(uri, server);

		
		File[] filesToLoads = new File(dir).listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.endsWith(".log");
//				return name.contains("172.16.100.21-") && name.endsWith(".log");
			}
		});
		if (filesToLoads == null) return;
		final CountDownLatch countDownLatch = new CountDownLatch(filesToLoads.length);
		for (final File file : filesToLoads) {
			final String address = file.getName().split("-")[0];
			Runnable runnable = new Runnable(){
				public void run() {
					try {
							RAF raf = RafFactory.getRaf(file.getAbsolutePath(), "");
							Socket socket = new Socket("localhost", 8991);
							
							OutputStream os = socket.getOutputStream();

							String line = "";
							while ((line = raf.readLine()) != null) {
//								server.receive((line + "\n").getBytes(), address, address);
								os.write((line + "\n").getBytes());
							}
						} catch (Exception e) {
							e.printStackTrace();
					} finally {
						System.out.println("Done:"  + address);
						countDownLatch.countDown();
					}
				}
			};
			executor.execute(runnable);
		}
		countDownLatch.await();
		
	}
	
	@Test
	public void shouldNotMergeStreams() throws Exception {
		final RawLogServer server = new RawLogServer("build/RawIntegTestMERGE");


		server.start();
		final int msgsToSend = 1000;
		final int clients = 10;
		ExecutorService executor = Executors.newCachedThreadPool();
		
		final CountDownLatch countDownLatch = new CountDownLatch(clients);
		
		for (int i = 0; i < clients; i++){
			final int clientId = i;
			final String remoteAddress = "0.0.0." + clientId;
			Runnable runnable = new Runnable(){
				public void run() {
					try {
						for (int j = 0; j< msgsToSend; j++) {
								server.receive(String.format("%s msg-%d-%d\n",new Date().toString(),clientId,j).getBytes(), remoteAddress, remoteAddress);
						}
						} catch (InterruptedException e) {
							e.printStackTrace();
					} finally {
						System.out.println("Done:"  + clientId);
						countDownLatch.countDown();
					}
				}
			};
			executor.execute(runnable);
		}
		countDownLatch.await();
		Thread.sleep(10 * 1000);
		
	}
	
	@Test
	public void shouldBreakDownFileCorrectly() throws Exception {
		System.setProperty("raw.server.fq.delay.secs", "1");
		RawLogServer server = new RawLogServer("build/RawIntegTest");
		server.start();
		
		String file = "test-data/mixed-logs.log";
//		String file = "test-data/SpunkCookedOSSEC-raw-11May09.log";
//		String file = "test-data/SplunkCookedASASession-raw-11May09.log";
		
		int c = 0;
		while (c++ < 1) {
			System.out.println("Running");
			RAF raf = RafFactory.getRaf(file, BreakRule.Rule.SingleLine.name());
			// test it to time for splunk files
//			raf.setBreakRule("Explicit:_raw");
			String line = "";
			
			long start = DateTimeUtils.currentTimeMillis();
			
			int lines = 0;
			while ((line = raf.readLine()) != null) {
//				System.out.println("\n\n****line:" + line);
//				line = "_raw" + line;
				lines++;
				server.receive((line + "\n").getBytes(), "127.0.0.0", "TestHost");
				
			}
			System.out.println("LinesTotal:" + lines);
			
			System.out.println("Elapsed:" + (DateTimeUtils.currentTimeMillis() - start));
		}
		
		Thread.sleep(2200);
		
//		assertTrue(new File("build/RawIntegTest/splunk").exists());
		assertTrue(new File("build/RawIntegTest/TestHost/127.0.0.0/ASA-session/192.168.3.1").exists());
		
		
	}

	@Test
	public void shouldRecogniseHTTP() throws Exception{
		final RawLogServer server = new RawLogServer("build/RawIntegTestHTTP");

		final URI uri = new URI("raw://localhost:19991");
		NettyEndPointFactory epFactory = new NettyEndPointFactory(com.liquidlabs.common.concurrent.ExecutorService.newScheduledThreadPool(3, "RawLogServer"), "logserver");
		epFactory.start();
		EndPoint endPoint = epFactory.getEndPoint(uri, server);



			server.start();
		final int msgsToSend = 10;
		final int clients = 10;
		ExecutorService executor = Executors.newCachedThreadPool();

		final CountDownLatch countDownLatch = new CountDownLatch(clients);

//		for (int i = 0; i < clients; i++){
//			final int clientId = i;
//			final String remoteAddress = "0.0.0." + clientId;
//			Runnable runnable = new Runnable(){
//				public void run() {
////					try {
//////						for (int j = 0; j< msgsToSend; j++) {
//////							server.receive(String.format("POST / HTTP/1.1\n" +
//////									"User-Agent: Stuff\n"  +
//////									"Host: 123.123.13\n"+
//////									"Accept:*/*\n" +
//////									"Content-Length: 29\n"     +
//////									"Content-Type: application/x-www-form-urlencoded"+
//////									"&path=Test&data=Test&host=Test").getBytes(), remoteAddress, remoteAddress);
//////						}
////					} catch (InterruptedException e) {
////						e.printStackTrace();
////					} finally {
////						System.out.println("Done:"  + clientId);
////						countDownLatch.countDown();
////					}
//				}
//			};
//			executor.execute(runnable);
//		}
		countDownLatch.await();
		Thread.sleep(1000 * 1000);
	}
}
