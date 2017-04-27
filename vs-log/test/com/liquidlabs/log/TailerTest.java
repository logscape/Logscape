package com.liquidlabs.log;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.indexer.krati.KratiIndexer;
import com.liquidlabs.log.reader.LocalIndexerIngestHandler;
import com.liquidlabs.log.reader.GenericIngester;
import com.liquidlabs.log.space.*;
import com.liquidlabs.log.test.LogLoadTester;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.resource.ResourceSpace;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

public class TailerTest {
	
	
	private LookupSpace lookupSpace;
	private ORMapperFactory factory;
	private LogSpace logSpace;
	private MyEventListener eventListener;
	private File log;
	private Indexer indexer;
	GenericIngester fileIngester;
	private LogLoadTester loadTester;
	private AggSpaceImpl aggSpace;
	private AtomicLong indexedDataSize = new AtomicLong();
	private ResourceSpace resourceSpace;
	private Mockery context;
	
	FieldSet fieldSet = FieldSets.get();
	
	
	public static class MyEventListener implements LogEventListener {
		int count = 0;
		List<String> events = new ArrayList<String>();
		List<LogEvent> events1 = new ArrayList<LogEvent>();
		
		public String getId() {
			return "my-listener";
		}

		public void handle(LogEvent event) {
			events.add(event.getMessage());
			events1.add(event);
			System.out.println("Test, Got LogEvent count:" + count++ + " s:" + events.size());
		}
		
	}
	
	@Before
	public void setUp() throws Exception {
		context = new Mockery();
		
		lookupSpace = context.mock(LookupSpace.class);
		
		context.checking(new Expectations() {
			{
				atLeast(1).of(lookupSpace).registerService(with(any(ServiceInfo.class)), with(any(long.class))); will(returnValue("xx"));
			}
			
		});
//		lookupSpace.stubs();
//		lookupSpace.stubs().method("unregisterService").will(returnValue(true));
		factory = new ORMapperFactory();
		ProxyFactoryImpl proxyFactory = factory.getProxyFactory();
		SpaceServiceImpl bucketService = new SpaceServiceImpl((LookupSpace) lookupSpace, factory, "AGGSPACE", proxyFactory.getScheduler(), false, false, false);
		SpaceServiceImpl replayService = new SpaceServiceImpl((LookupSpace) lookupSpace, factory, "AGGSPACE", proxyFactory.getScheduler(), false, false, false);
		SpaceServiceImpl logEventSpaceService = new SpaceServiceImpl((LookupSpace) lookupSpace, factory, "AGGSPACE", proxyFactory.getScheduler(), false, false, true);
		
		aggSpace = new AggSpaceImpl("providerId", bucketService, replayService, logEventSpaceService, proxyFactory.getScheduler());
		aggSpace.start();
		
		SpaceServiceImpl spaceServiceImpl = new SpaceServiceImpl((LookupSpace)lookupSpace,factory, LogSpace.NAME, factory.getProxyFactory().getScheduler(), false, false, true);
		logSpace = new LogSpaceImpl(spaceServiceImpl, spaceServiceImpl, null, aggSpace, null, null, null, resourceSpace, lookupSpace);
		logSpace.start();
		
		
		eventListener = new MyEventListener();
		aggSpace.registerEventListener(eventListener, eventListener.getId(), null, -1);
		new File("build").mkdir();
		
		log = new File("build","TailerTest.out");
		
		loadTester = new LogLoadTester("test-data/trade-load-template.log", log.getAbsolutePath(), "yyyy-MM-dd HH:mm:ss,SSS", false);
//		loadTester.writeBurst(10, 10 * 4 * (5 * 600 * 10));
		loadTester.writeBurst(10, 600 * 10);
		
		indexer = new KratiIndexer("build/TailerTest");

	}

	@After
	public void tearDown() throws Exception {
		try {
			logSpace.stop();
			factory.stop();
			indexer.close();
			FileUtil.deleteDir(new File("build/TailerTest"));
		}catch(Throwable t) {
			t.printStackTrace();
		}
	}
	
	
	@Test
	public void testShouldBeAbleToDeleteFileToCheckWin32DoesntLockTheFile() throws Exception {
		System.out.println("Starting................");
		File newToFile = new File("build", "tailerTestCanDeleteMe.log");
		FileUtil.copyFile(log, newToFile);
		
		LogFile logFile = indexer.openLogFile(newToFile.getAbsolutePath(), true, fieldSet.getId(), "");
		
//		fileIngester = new FileIngester(logFile, aggSpace, disco,"sourceUri", "hostname", new WatchDirectory(), indexedDataSize);
        fileIngester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
		TailerImpl logTailer = new TailerImpl(newToFile, 0, 0, fileIngester, new WatchDirectory(), indexer);
		fileIngester.setFilters(new String [] {".*WARN.*"}, new String[0]);
		logTailer.call();
		Thread.sleep(100);
		
		boolean isDeleteSuccessful = newToFile.delete();
		assertTrue("Could Not Delete the file:" + newToFile.getName(), isDeleteSuccessful);
	}
	@Test
	public void testShouldStartFromBeginningWhenSpecified() throws Exception {
		System.out.println("Starting................");
		
        LogFile logFile = indexer.openLogFile(FileUtil.getPath(log), true, fieldSet.getId(), "");

        fileIngester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
        TailerImpl logTailer = new TailerImpl(log, 100, 2, fileIngester, new WatchDirectory(), indexer);
		fileIngester.setFilters(new String [] {".*WARN.*"}, new String[0]);
		logTailer.call();
		long[] lastPosAndLastLine = indexer.getLastPosAndLastLine(FileUtil.getPath(log));
		
		System.out.println("LastLine:" + lastPosAndLastLine[1]);
		assertTrue(lastPosAndLastLine[0] > 0);
		assertTrue(lastPosAndLastLine[1] > 0);
		
		
		Thread.sleep(500);
		
		// returns 0 - cause it checks the age of the line - if its too old it gets ignored
//		assertEquals(37, eventListener.events.size());
	}
	
	

}
