package com.liquidlabs.log.search;

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
import com.liquidlabs.log.space.LogEvent;
import com.liquidlabs.log.space.WatchDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class SearchPicksupLiveEventsTest {
	
	NullAggSpace aggSpace;
	NullLogSpace logSpace;
	private File dbDir;
	private File log;
	PrintWriter pw;
	LogReader writer;
	TailerImpl tailer;
	private Thread writerThread;

	protected Indexer indexer;

	private FieldSet fieldSet;
	
	@Before
	public void setUp() throws Exception {
		
		try {
		
			logSpace = new NullLogSpace();
			aggSpace = new NullAggSpace();
			String dbName = "build/" + getClass().getSimpleName();
			dbDir = new File(dbName);
			FileUtil.deleteDir(dbDir);
			dbDir.mkdirs();
			log = new File("build/" + getClass().getSimpleName() + ".log");
			if (log.exists()) {
				System.err.println("LogExists!");
			}
			
			pw = new PrintWriter(log);
			setupIndexer();
			
			fieldSet = new FieldSet("(s28) (**)", "timestamp", "data");
			fieldSet.id = "testFieldSet";
			fieldSet.addSynthField("memAvail", "data", "AVAIL:(d)","count(*)", true, true);
			fieldSet.addSynthField("hostname", "data", "AGENT (*)","count(*)", true, true);
			indexer.addFieldSet(fieldSet);
			
			LogFile logFile = indexer.openLogFile(log.getAbsolutePath(), true, fieldSet.id, "");
			
			WatchDirectory watch = new WatchDirectory();
			writerThread = createWriter(pw);
			writerThread.start();
			Thread.sleep(1000);
			
            writer = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", watch);
			tailer = new TailerImpl(log,0, 0,writer, watch, indexer);
			
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	protected void setupIndexer() {
		indexer = new KratiIndexer(this.dbDir.getAbsolutePath());
	}
	
	@After
	public void tearDown() throws Exception {
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
	public void testShouldEmitLiveEvents() throws Exception {
		    
		LogFile openLogFile = indexer.openLogFile(log.getAbsolutePath(), true, fieldSet.id, "");

		WatchDirectory watch = new WatchDirectory();
		watch.setMaxAge(99999);

        writer = new GenericIngester(openLogFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", watch);

		TailerImpl tailer = new TailerImpl(log, 0,0, writer, watch, indexer);
		tailer.setFilters(new String[] { "ABC", "Engine", "CPU" }, new String[] { "line=1" });
		
		// call the first time cause they arent raised on import
		tailer.call();
		Thread.sleep(3000);
		tailer.call();
		List<LogEvent> events = aggSpace.events;
		for (LogEvent logEvent : events) {
			System.out.println(logEvent);
		}
		assertTrue("Didnt get any live events!", events.size() > 0);
		
	}
	private int agentLines;
	private int engineLines;
	
	private Thread createWriter(final PrintWriter pw) {
		Thread writerThread = new Thread(new Runnable() {

			public void run() {
				agentLines = 0;
				engineLines = 0;
				boolean interrupted = false;
				int i = 1;
				int memAvail = 68;
				Random rand = new Random();
				while(!Thread.currentThread().isInterrupted() && !interrupted) {
					if (i % 5 == 0) {
						pw.print(new Date() + " Engine line = " + i++);
						pw.println("\nEngine line = " + i++);
						engineLines++;
						pw.flush();
					} else {
						if (i % 2 == 0) {
							memAvail = 0;
						} else {
							memAvail = 68;
						}
						pw.println(new Date() + String.format(" AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:%d - line=%d", memAvail, i++));
						pw.println(new Date() + " AGENT alteredcarbon.local-12050 CPU:6.000000 - line=" + i);
						pw.flush();
						agentLines += 2;
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
