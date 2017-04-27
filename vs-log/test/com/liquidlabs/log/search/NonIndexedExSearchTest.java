package com.liquidlabs.log.search;

import com.liquidlabs.common.UID;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.LogReader;
import com.liquidlabs.log.NullAggSpace;
import com.liquidlabs.log.NullLogSpace;
import com.liquidlabs.log.TailerImpl;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.indexer.krati.KratiIndexer;
import com.liquidlabs.log.reader.LocalIndexerIngestHandler;
import com.liquidlabs.log.reader.GenericIngester;
import com.liquidlabs.log.search.filters.Contains;
import com.liquidlabs.log.search.handlers.SearchHistogramHandler;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.log.space.WatchDirectory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NonIndexedExSearchTest {
	
	NullAggSpace aggSpace;
	NullLogSpace logSpace;
	private File dbDir;
	private File log;
	PrintWriter pw;
	private Thread writerThread;

	protected Indexer indexer;
	protected List<HistoEvent> histo;

	protected AtomicLong indexedDataSize = new AtomicLong();
	protected int limit = Integer.MAX_VALUE;
	protected AtomicInteger sent = new AtomicInteger();
	private FieldSet fieldSet;

	
	@Before
	public void setUp() throws Exception {
		
		logSpace = new NullLogSpace();
		aggSpace = new NullAggSpace();
		String dbName = "build/LogFileScannerRealTest";
		dbDir = new File(dbName);
		FileUtil.deleteDir(dbDir);
		dbDir.mkdirs();
		log = new File("build/a.log");
		if (log.exists()) {
			System.err.println("LogExists!");
		}
		
		pw = new PrintWriter(log);
		setupIndexer();
		writerThread = createWriter(pw);
		writerThread.start();
		
		histo = new ArrayList<HistoEvent>();
	}

	protected void setupIndexer() {
		indexer = new KratiIndexer("build/LogFileScannerRealTest");
	}
	
	@After
	public void tearDown() throws Exception {
		log.delete();
		writerThread.interrupt();
		writerThread.join(5000);
		if (writerThread.isAlive()) {
			System.out.println("Writer Thread still alive!");
			writerThread.interrupt();
		}
		indexer.close();
		FileUtil.deleteDir(dbDir);

	}

    @Test
    public void shouldDoSquat() {
        // placeholder
    }

    //	@Test DodgyTest?
	public void shouldMatchRealMLineQuery() throws Exception {
		File f = new File("test-data/mline-2.log");
		fieldSet = new FieldSet("(s23) (*) (*) (**)","timestamp", "module", "type", "text");
		fieldSet.id = UID.getUUID();
		
		indexer.addFieldSet(fieldSet);
		WatchDirectory watch = new WatchDirectory();
		watch.setMaxAge(9999);
		
		LogFile logFile = indexer.openLogFile(f.getAbsolutePath(), true, fieldSet.id, "");

        LogReader writer = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
//		writer.setForceLiveEvents(true);
		
		TailerImpl theTailer = new TailerImpl(f, 0,0, writer, new WatchDirectory(), indexer);
		
		theTailer.call();
		
		LogRequest request = getRequest(new DateTime().minusYears(2).getMillis(),new DateTime().getMillis(), fieldSet.id, "text", "Exception");
		createScanner(request, f, logSpace, indexer).search(request, histo, sent);
		
		Assert.assertEquals("No AggEvents", 1, aggSpace.buckets.size());
		Assert.assertEquals("No Hits", 2, aggSpace.buckets.get(0).hits());
	}

	protected Scanner createScanner(LogRequest request, File file, LogSpace logSpace, Indexer indexer) throws IOException {
		return new FileScanner(indexer, file.getAbsolutePath(), new SearchHistogramHandler(null, new LogFile("file",1, "basic", "tags"), request), "", "", aggSpace);
	}

	protected LogRequest getRequest(long from, long to, String fieldSetId, String fieldName, String... patterns) {
		LogRequest request = new LogRequest("sub", from, to);
		int i = 0;
		for (String string : patterns) {
			Query query = new Query(i++, i, string, string, false);
			query.addFilter(new Contains("","text",Arrays.asList(new String[] {"Exception" } )));
			request.addQuery(query);
		}
		
		request.setVerbose(true);
		
		setupHisto(request);
		return request;
	}

	private void setupHisto(LogRequest request) {
		this.histo = new ArrayList<HistoEvent>();
		for (Query query : request.queries()) {
			histo.add(new HistoEvent(request, 0, 0, DateTimeUtils.currentTimeMillis(), 1, "hostname", "endPoint", request.subscriber(), query, aggSpace, false));
		}
	}

	

	private Thread createWriter(final PrintWriter pw) {
		Thread writerThread = new Thread(new Runnable() {
			public void run() {
				boolean interrupted = false;
				int i = 1;
				Random rand = new Random();
				while(!Thread.currentThread().isInterrupted() && !interrupted) {
					if (i % 5 == 0) {
						pw.print("Engine line = " + i++);
						pw.flush();
						pw.println("\nEngine line = " + i++);
						pw.flush();
					} else {
					pw.println("AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68 - line=" + i++);
					pw.println("AGENT alteredcarbon.local-12050 CPU:6.000000 - line=" + i++);
					pw.flush();
					}
					try {
						Thread.sleep(rand.nextInt(500));
					} catch (InterruptedException e) {
						interrupted = true;
					}
				}
				System.out.println("Writer Thread is Terminating term:" + interrupted);
			}});
		
		writerThread.setDaemon(true);
		return writerThread;
	}

}
