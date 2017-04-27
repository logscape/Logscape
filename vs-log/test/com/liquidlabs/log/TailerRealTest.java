package com.liquidlabs.log;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.indexer.krati.KratiIndexer;
import com.liquidlabs.log.reader.LocalIndexerIngestHandler;
import com.liquidlabs.log.reader.GenericIngester;
import com.liquidlabs.log.space.WatchDirectory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.containsString;

public class TailerRealTest {
	
	private NullAggSpace aggSpace;
	private File dbDir;
	private File log;
	private PrintWriter pw;
	private LogReader writer;
	private TailerImpl tailer;
	private Thread writerThread;

	private Indexer indexer;

	String dbName = "build/TailerRealTest_" + new DateTime().getSecondOfMinute();
	private AtomicLong indexedDataSize = new AtomicLong();
	
	public long interval = 500;

	@Before
	public void setUp() throws Exception {
		
		try {
			FileUtil.deleteDir(dbDir);
		
			aggSpace = new NullAggSpace();
			dbDir = new File(dbName);
			dbDir.mkdirs();
			
			log = new File(dbName + "-agent.log");
			pw = new PrintWriter(log);
			writerThread = createWriter(pw);
			writerThread.start();
			Thread.sleep(1000);
			indexer = new KratiIndexer(dbName);
			
			FieldSet fieldSet = FieldSets.get();
			indexer.addFieldSet(fieldSet);
			
			LogFile logFile = indexer.openLogFile(log.getAbsolutePath(), true, fieldSet.getId(), "");
            writer = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
			tailer = new TailerImpl(log,0, 0,writer, new WatchDirectory(), indexer);

			
			
		} catch (Throwable t ){
			t.printStackTrace();
		}
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
		log.delete();
	}
	
	
//	@Test DodgyTest?: Should it? THe code doesn't reset it. Does it do this deliberately?
	public void testShouldRestartWhenRollUnsuccessfulDetectedAndLogFileExists() throws Exception {
		Thread.sleep(1000);
		
		// get tailer to the end of the file
		TailResult result1 = tailer.call();
	
		// should have a bunch of lines by now
		assertEquals(TailResult.DATA, result1);
		assertTrue("Expected to get 5 lines in tailer but got:" + tailer.line, tailer.line > 5);
		long[] lastPosAndLastLine1 = indexer.getLastPosAndLastLine(log.getAbsolutePath());
		assertTrue("disco has wrong linecount", lastPosAndLastLine1[1] > 5);
		
		assertEquals("Expected line count to be different", tailer.line, lastPosAndLastLine1[1] + 1);
		
		
		// kill the tailer 
		writerThread.interrupt();
		writerThread.join();
		pw.close();
		
		System.out.println(">>>Last Tailer Line:::: " + tailer.line);
		
		// now truncate/rewrite the file contents
		FileOutputStream newFos = new FileOutputStream(log);
		newFos.write("1 new line\r\n".getBytes());
		newFos.write("2 new line\r\n".getBytes());
		newFos.close();
		
		// run the tailer to check it stops tailing and resets to 1 line in length
		TailResult result2 = tailer.call();
		assertEquals(TailResult.DATA, result2);
		
		assertEquals("next expected line was incorrect", 3, tailer.line);
		long[] lastPosAndLastLine2 = indexer.getLastPosAndLastLine(log.getAbsolutePath());
		assertEquals("disco has wrong linecount", 1,lastPosAndLastLine2[1]);
		
		assertTrue("Indexer should have had lower line count on second run:",lastPosAndLastLine2[1] < lastPosAndLastLine1[1]);
		
		System.out.println("done..........");
	}

    @Test
    public void should() throws Exception{
        final String blah = "2013-11-10 this is the the data for line no ";
        final String file = new File("build/foo.bar.log").getAbsolutePath();
        final FileOutputStream fos = new FileOutputStream(file);
        for(int i = 1;i<=10; i++) {
            fos.write((blah + i + "\n").getBytes());
        }
        fos.write((blah + 11).getBytes());
        fos.flush();

        FieldSet fieldSet = FieldSets.get();
        LogFile logFile = indexer.openLogFile(file, true, fieldSet.getId(), "");
        final WatchDirectory watch = new WatchDirectory();
        writer = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
        tailer = new TailerImpl(new File(file),0, 0, writer, watch, indexer);
        tailer.call();

        fos.write(" abcdefghi\n".getBytes());
        for(int i = 12; i<=20;i++) {
            fos.write((blah + i + "\n").getBytes());
        }

        fos.flush();
        tailer.call();

        final List<Line> lines = indexer.linesForNumbers(file, 1, 20);
        final RAF raf = RafFactory.getRafSingleLine(file);
        int i = 1;
        for (Line line : lines) {
            raf.seek(line.filePos);
            final String read = raf.readLine();
            assertThat(read, is(containsString(blah + i)));
        }

    }

    @Test
	public void testShouldWriteLastPartialLine() throws Exception {
		String filename = new File("build/tailerTestPartialLLine.log").getAbsolutePath();
		
		FileOutputStream fos = new FileOutputStream(filename);
		fos.write("line 1\nline 2\nline 3\nline 4 partial".getBytes());
		fos.close();
		
		FieldSet fieldSet = FieldSets.get();
		
		LogFile logFile = indexer.openLogFile(filename, true, fieldSet.getId(), "");
		

        writer = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
		tailer = new TailerImpl(new File(filename),0, 0, writer, new WatchDirectory(), indexer);
		tailer.call();
		long[] lastPosAndLastLine = indexer.getLastPosAndLastLine(new File(filename).getAbsolutePath());
		// we cannot handle incomplete events - raf.readLine() will see EOF when incomplete line found - ! - so expected value is 3 and not 4
		assertEquals("line count was:" + lastPosAndLastLine[1], 4, lastPosAndLastLine[1]);
	}
	
	@Test
	public void testShouldDetectFileDeleteAndStopTailing() throws Exception {
		Thread.sleep(1000);
		
		// get tailer to the end of the file
		TailResult result1 = tailer.call();
		
		assertEquals(TailResult.DATA, result1);
		
		// kill the tailer
		writerThread.interrupt();
		writerThread.join();
		pw.close();
		
		// delete the file
		
		boolean delete = log.delete();
		System.out.println(">>>>>>>>>>>>> Deleted:" + delete);
		
		// run the tailer to check it stops tailing
		TailResult result2 = tailer.call();
		assertEquals(TailResult.FAILED, result2);
	}

    @Test
    public void shouldNotHoldOnToFirstLineWhenNoRollDetector() throws IOException {
        final String filename = new File("build/logwithbigfirstline.log").getAbsolutePath();
        final FileOutputStream fileOutputStream = new FileOutputStream(filename);
        fileOutputStream.write("2014-08-19 - INFO - This is shome shit that i don't care about again and again and again".getBytes());
        fileOutputStream.write("\n".getBytes());
        fileOutputStream.close();

        FieldSet fieldSet = FieldSets.get();

        LogFile logFile = indexer.openLogFile(filename, true, fieldSet.getId(), "");
        LogReader reader = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", new WatchDirectory());
        final TailerImpl theTailer = new TailerImpl(new File(filename), 0, 0, reader, new WatchDirectory(), indexer);
        theTailer.call();
        assertThat(theTailer.firstFileLineForRollDetection, is(""));
    }

    private Thread createWriter(final PrintWriter pw) {
		Thread writerThread = new Thread(new Runnable() {
			public void run() {
				boolean interrupted = false;
				int i = 1;
				org.joda.time.format.DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSS");
				while(!Thread.currentThread().isInterrupted() && !interrupted) {
					DateTime now = new DateTime();
					if (i % 5 == 0) {
						pw.print(formatter.print(now) + " INFO Engine line = " + i++);
						pw.println("\nEngine line = " + i++);
					} else {
						pw.println(formatter.print(now) + " AGENT alteredcarbon.local-12050-0 MEM MB MAX:92 COMMITED:32 USED:23 AVAIL:68 - line=" + i++);
						pw.println(formatter.print(now) + " AGENT alteredcarbon.local-12050 CPU:6.000000 - line=" + i++);
					}
					pw.flush();
					try {
						if (interval > 0) Thread.sleep(interval);
						else {
							Thread.yield();
							Thread.yield();
							Thread.yield();
							Thread.yield();
						}
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
