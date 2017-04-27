package com.liquidlabs.log.indexer;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.UID;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.common.regex.FileMaskAdapter;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.*;
import com.liquidlabs.log.indexer.persistit.PIIndexer;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.log.util.DateTimeExtractor;
import com.liquidlabs.vso.agent.metrics.DefaultOSGetter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;

public class PIBigIndexerTest {


	Indexer indexer;

	private int logId;
	private int limit;
	private FieldSet fieldSet;
	private String fieldSetId;
	private String sourceTags = "";
	private volatile static long dir =1;
	String DIR = "build/PIBIGIndexerTest" + dir++ ;
    private static final String LOG_FILE = "build/myFile" + dir + ".log";

    static ReentrantLock reentrantLock = new ReentrantLock();


    @Before
	public void setUp() throws Exception {
		try {

            reentrantLock.lock();
            com.liquidlabs.common.concurrent.ExecutorService.setTestMode();

            //Thread.sleep((long) (4000 * dir));
			String DIR = "build/PIBIGIndexerTest" + dir;
			System.out.println("Setup! DIRRRR:" + dir);
			DIR += System.currentTimeMillis();
            FileUtil.deleteDir(new File(DIR));
            new File(DIR).mkdirs();
			
			fieldSet = new FieldSet("(**)","data");
			fieldSet.id = UID.getUUID();
			fieldSetId = fieldSet.id;

			getIndexer();
			logId = indexer.open(LOG_FILE, true, fieldSetId, sourceTags );
			File file = new File(LOG_FILE);
			file.createNewFile();
			FileOutputStream fos = new FileOutputStream(file);
			fos.write("stuff".getBytes());
			fos.close();
		} catch (Throwable t) {
			t.printStackTrace();
            throw new RuntimeException(t);
		}
	}

	protected void getIndexer() {
		indexer = new PIIndexer(DIR);
	}

	@After
	public void tearDown() throws Exception {
		if (indexer == null) {
			System.err.println("Indexer is null!");
		} else {
			indexer.close();

            System.gc();
		}
       // FileUtil.deleteDir(new File(DIR));
        reentrantLock.unlock();

    }
	
	
	@Test
	public void shouldGetGoodIndexStatsData() throws Exception {
		File file = new File("build/bigIndexer/test/index-test/A/agent.log");
		file.getParentFile().mkdirs();
		FileUtil.copyFile(new File("test-data/agent.log"), file);
		importFile(file);
		
		File file2 = new File("build/bigIndexer/test/index-test/B/agent2.log");
		FileUtil.copyFile(new File("test-data/agent.log"), file2);
		importFile(file2);

		FieldSet myFieldSet = FieldSets.getLog4JFieldSet();
		myFieldSet.lastModified = 1000;
		myFieldSet.fileNameMask = "*.log*";
		indexer.addFieldSet(myFieldSet);
		
		// check we got 2 files with this type
		indexer.openLogFile(file.getAbsolutePath());
		indexer.openLogFile(file2.getAbsolutePath());
		
		IndexStats indexStats = indexer.indexStats();
		assertTrue(indexStats.totalTags() > 0);
		
	}
	@Test
	public void shouldAssignFieldSetThenUnAssignOnUpdateWithExcludeMask() throws Exception {
		File file = new File("build/bigIndexer/test/exlc-test/A/agent.log");
		file.getParentFile().mkdirs();
		FileUtil.copyFile(new File("test-data/agent.log"), file);
		importFile(file);
		
		File file2 = new File("build/bigIndexer/test/excl-test/B/agent2.log");
		FileUtil.copyFile(new File("test-data/agent.log"), file2);
		importFile(file2);

		FieldSet myFieldSet = FieldSets.getLog4JFieldSet();
		myFieldSet.lastModified = 1000;
		myFieldSet.fileNameMask = "*.log*";
        myFieldSet.filePathMask = "**";
		indexer.addFieldSet(myFieldSet);
		
		// check we got 2 files with this type
		LogFile f1 = indexer.openLogFile(file.getAbsolutePath());
		System.out.println("File:" + f1);
		assertEquals(myFieldSet.id, f1.getFieldSetId());
		
		LogFile f2 = indexer.openLogFile(file2.getAbsolutePath());
		assertEquals(myFieldSet.id, f2.getFieldSetId());
		
		// now update the fieldSet with a more restrictive mask
		
		myFieldSet.fileNameMask = "*.log*,!2";
		myFieldSet.lastModified = 2000;
		indexer.addFieldSet(myFieldSet);
		
		// now check those that we in the first one are no longer included
		LogFile f11 = indexer.openLogFile(file.getAbsolutePath());
		assertEquals(myFieldSet.id, f11.getFieldSetId());
		
		LogFile f22 = indexer.openLogFile(file2.getAbsolutePath());
		assertFalse("Should Have removed the type mapping", myFieldSet.id.equals(f22.getFieldSetId()));
	}

	
	@Test
	public void shouldAssignFieldSetThenUnAssignOnUpdate() throws Exception {

        FileUtil.copyFile(new File("test-data/agent.log"), new File("test-data/agent-unass.log"));
        FileUtil.copyFile(new File("test-data/agent2.log"), new File("test-data/agent-unass2.log"));

		File file = new File("test-data/agent-unass.log");
		importFile(file);
		
		File file2 = new File("test-data/agent-unass2.log");
		importFile(file2);

		FieldSet myFieldSet = FieldSets.getLog4JFieldSet();
        myFieldSet.id = "unass-fs";
		myFieldSet.lastModified = 1000;
		myFieldSet.fileNameMask = "*.log";
        myFieldSet.filePathMask  = "**";
		indexer.addFieldSet(myFieldSet);
		
		// check we got 2 files with this type
		LogFile f1 = indexer.openLogFile(file.getAbsolutePath());
        System.out.println("File:" + f1);
        assertEquals(myFieldSet.id, f1.getFieldSetId());
		
		LogFile f2 = indexer.openLogFile(file2.getAbsolutePath());
		assertEquals(myFieldSet.id, f2.getFieldSetId());
		
		// now update the fieldSet with a more restrictive mask
		
		myFieldSet.fileNameMask = "*agent*.log";
		myFieldSet.lastModified = 2000;
		indexer.addFieldSet(myFieldSet);
		
		// now check those that we in the first one are no longer included
		LogFile f11 = indexer.openLogFile(file.getAbsolutePath());
		assertEquals(myFieldSet.id, f11.getFieldSetId());
		
		LogFile f22 = indexer.openLogFile(file2.getAbsolutePath());
		assertEquals("Should Have removed the type mapping", f22.getFieldSetId(), myFieldSet.id);
	}
	
	@Test
	public void shouldAssignFieldSetToLogFile() throws Exception {
		File file = new File("test-data/agent.log");
		importFile(file);
        FieldSet log4jFS = FieldSets.getLog4JFieldSet();
        log4jFS.filePathMask = "**";
        indexer.addFieldSet(log4jFS);
		LogFile now = indexer.openLogFile(file.getAbsolutePath());
		assertEquals("log4j", now.getFieldSetId());
		
		FieldSet lowerPriorityLog4j = FieldSets.getLog4JFieldSet();
		lowerPriorityLog4j.id = "craplog4j";
		lowerPriorityLog4j.priority = lowerPriorityLog4j.priority - 10;
        lowerPriorityLog4j.filePathMask = "**";
		indexer.addFieldSet(lowerPriorityLog4j);
		
		LogFile now2 = indexer.openLogFile(file.getAbsolutePath());
		assertEquals("log4j", now2.getFieldSetId());

		
		
	}

	private void importFile(File file) throws FileNotFoundException,
			IOException {
		indexer.open(file.getAbsolutePath(), true, "basic", "test");
		RAF raf = RafFactory.getRafSingleLine(file.getAbsolutePath());
		String line = "";
		DateTimeExtractor extractor = new DateTimeExtractor();
		while ((line = raf.readLine()) != null) {
			indexer.add(file.getAbsolutePath(),  raf.linesRead(), extractor.getTime(line, file.lastModified()).getTime(), raf.getFilePointer());
		}
		raf.close();
	}
	
	@Test
	public void testRemoveFromIndex() throws Exception {
		DateTime dateTimeONE = new DateTime().minusHours(1);

		indexer.open("one", true, fieldSetId, sourceTags);
		indexer.add("one", 100, dateTimeONE.getMillis(), 100);
		indexer.open("two", true, fieldSetId, sourceTags);
		indexer.add("two", 100, dateTimeONE.getMillis(), 100);
		indexer.open("three", true, fieldSetId, sourceTags);
		indexer.add("three", 100, dateTimeONE.getMillis(), 100);
		
		int firstCount = indexer.indexedFiles(0, System.currentTimeMillis(),false, LogFileOps.FilterCallback.ALWAYS).size();
		
		indexer.removeFromIndex("", "one", true);
		int secondCount = indexer.indexedFiles(0, System.currentTimeMillis(),false, LogFileOps.FilterCallback.ALWAYS).size();
		assertEquals(firstCount-1, secondCount);
		
		indexer.removeFromIndex("", ".*", true);
		
		Collection<LogFile> indexedFiles = indexer.indexedFiles(0, System.currentTimeMillis(),false, LogFileOps.FilterCallback.ALWAYS);
		assertEquals(0, indexedFiles.size());
	}
	
	
	@Test
	public void testBulkRemoveFromIndex() throws Exception {
		DateTime dateTimeONE = new DateTime().minusHours(1);

		indexer.open("one-bulk", true, fieldSetId, sourceTags);
		indexer.add("one-bulk", 100, dateTimeONE.getMillis(), 100);
		indexer.open("two-bulk", true, fieldSetId, sourceTags);
		indexer.add("two-bulk", 100, dateTimeONE.getMillis(), 100);
		indexer.open("three-bulk", true, fieldSetId, sourceTags);
		indexer.add("three-bulk", 100, dateTimeONE.getMillis(), 100);
		
		List<LogFile> indexedFiles2 = indexer.indexedFiles(0, System.currentTimeMillis(), false, new LogFileOps.FilterCallbackRegEx("bulk"));
		
		assertEquals(3, indexedFiles2.size());
		int removedCount = indexer.removeFromIndex(indexedFiles2);
		//assertEquals(6,  removedCount);
		
		List<LogFile> indexedFiles3 = indexer.indexedFiles(0, System.currentTimeMillis(), false, new LogFileOps.FilterCallbackRegEx("bulk"));
		assertEquals(0, indexedFiles3.size());
		
	}
	
	@Test
	public void shouldListFilesUsingFilterCallback() throws Exception {
		DateTime dateTimeONE = new DateTime().minusHours(1);

		indexer.open("/var/logs/one.log", true, fieldSetId, "yes");
		indexer.add("/var/logs/one.log", 100, dateTimeONE.getMillis()-100, 100);
		indexer.open("/var/logs/two.log", true, fieldSetId, "maybe,yes");
		indexer.add("/var/logs/two.log", 100, dateTimeONE.getMillis()-90, 100);
		indexer.open("/var/logs/three.log", true, fieldSetId, "no");
		indexer.add("/var/logs/three.log", 100, dateTimeONE.getMillis()-80, 100);
		
		Collection<LogFile> files1 = indexer.indexedFiles(0L, System.currentTimeMillis(), true, new LogFileOps.FilterCallback() {
			public boolean accept(LogFile logFile) {
				return logFile.getFileName().contains("one");
			}
		});
		for (LogFile logFile : files1) {
			assertTrue(logFile.getFileName().contains("one"));
		}
		Collection<LogFile> files2 = indexer.indexedFiles(0L, System.currentTimeMillis(), true, new LogFileOps.FilterCallback() {
			public boolean accept(LogFile logFile) {
				return true;
			}
		});
		assertTrue(files2.size() > files1.size());

		
	}
	
	
	@Test
	public void shouldRetieveFilesWithWatchMask() throws Exception {
		DateTime dateTimeONE = new DateTime().minusHours(1);

		indexer.open("/var/logs/one.log", true, fieldSetId, "yes");
		indexer.add("/var/logs/one.log", 100, dateTimeONE.getMillis()-100, 100);
		indexer.open("/var/logs/two.log", true, fieldSetId, "maybe,yes");
		indexer.add("/var/logs/two.log", 100, dateTimeONE.getMillis()-90, 100);
		indexer.open("/var/logs/three.log", true, fieldSetId, "no");
		indexer.add("/var/logs/three.log", 100, dateTimeONE.getMillis()-80, 100);
		WatchDirectory watch = new WatchDirectory("tafs", "/var/logs","*","","", 100, "", true,"", "", false, true);

		Collection<LogFile> indexedFiles1 = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx(watch.getDirNameForRegExp() + ".*"));
		Collection<LogFile> indexedFiles2 = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx(watch.getDirNameForRegExp() + ".*"));
		assertEquals("Didnt get same file count for same call", indexedFiles1.size(), indexedFiles2.size());
		
		Collection<LogFile> indexedFiles3 = indexer.indexedFiles(0, Long.MAX_VALUE, false,  new LogFileOps.FilterCallback() {
			@Override
			public boolean accept(LogFile logFile) {
				return logFile.getFileName().contains("one");
			}
		});
		assertEquals("Didnt get ONE file = got:" + indexedFiles3, 1, indexedFiles3.size());
	}
	
	
	@Test
	public void shouldAllowTagsAndFileMaskMixing() throws Exception {
		DateTime dateTimeONE = new DateTime().minusHours(1);

		indexer.open("one1", true, fieldSetId, "yes1");
		indexer.add("one1", 100, dateTimeONE.getMillis()-100, 100);
		indexer.open("two1", true, fieldSetId, "maybe1,yes1");
		indexer.add("two1", 100, dateTimeONE.getMillis()-90, 100);
		indexer.open("three1", true, fieldSetId, "no1");
		indexer.add("three1", 100, dateTimeONE.getMillis()-80, 100);

		Collection<LogFile> indexedFiles = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("tag:maybe1,.*one1.*"));
		assertEquals(indexedFiles.toString(), 2, indexedFiles.size());
	}

	@Test
	public void shouldListFilesWithMatchingTags() throws Exception {
		DateTime dateTimeONE = new DateTime().minusHours(1);

		indexer.open("one2", true, fieldSetId, "yes2");
		indexer.add("one2", 100, dateTimeONE.getMillis()-100, 100);
		indexer.open("two2", true, fieldSetId, "maybe2,yes2");
		indexer.add("two2", 100, dateTimeONE.getMillis()-90, 100);
		indexer.open("three2", true, fieldSetId, "no2");
		indexer.add("three2", 100, dateTimeONE.getMillis()-80, 100);

		Collection<LogFile> indexedFiles = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("tag:yes2"));
		assertEquals(2, indexedFiles.size());

		Collection<LogFile> indexedFiles2 = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("tag:maybe2"));
		assertEquals(1, indexedFiles2.size());

		Collection<LogFile> indexedFiles3 = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("tag:maybe2,tag:no2"));
		assertEquals(2, indexedFiles3.size());

	}
	
	@Test
	public void testListFilesWithRegExp() throws Exception {
		long now = new DateTime().minusHours(1).getMillis();

		indexer.open("one", true, fieldSetId, sourceTags);
		indexer.add("one", 100, now -100, 100);
		indexer.open("two", true, fieldSetId, sourceTags);
		indexer.add("two", 100, now -90, 100);
		indexer.open("three-A", true, fieldSetId, sourceTags);
		indexer.add("three-A", 100, now -80, 100);
		indexer.open("three-B", true, fieldSetId, sourceTags);
		indexer.add("three-B", 100, now -70, 100);
		
		Collection<LogFile> indexedFiles = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx(".*three.*"));
		assertEquals(2, indexedFiles.size());
		
		List<LogFile> indexedFiles2 = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("three"));
		assertEquals(2, indexedFiles2.size());
		assertTrue(indexedFiles2.toString().contains("three-B"));
		
	}
	@Test
	public void testListFilesWithWindowsStyleNaming() throws Exception {
		DateTime dateTimeONE = new DateTime().minusHours(1);
		String dir = "C:\\Program Files\\Apache Group\\Apache2\\logs";
		String file = "\\lon.error";
		indexer.open(dir + file, true, fieldSetId, sourceTags);
		indexer.add(dir + file, 100, dateTimeONE.getMillis(), 100);
		
		List<LogFile> indexedFiles =indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx(dir));
		assertEquals(1, indexedFiles.size());
	}
	
	@Test
	public void testListFilesWithRegExp2() throws Exception {
		DateTime dateTimeONE = new DateTime().minusHours(1);
		String dir = "C:\\Program Files\\Apache Group\\Apache2\\logs";
		String file = "\\lon.error";
		indexer.open(dir + file, true, fieldSetId, sourceTags);
		indexer.add(dir + file, 100, dateTimeONE.getMillis(), 100);
		
		//.*\work.*\\.evt
		List<LogFile> indexedFiles = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx(".*\\\\Apache.*\\.error"));
		assertEquals(1, indexedFiles.size());
		
		// see if she blows
		String dot = FileUtil.getPath(new File("."));
		dot = FileMaskAdapter.adapt(dot, DefaultOSGetter.isA());
		indexedFiles = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx(dot + ".*"));
		indexedFiles = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("./work" + ".*"));
		dir = FileMaskAdapter.adapt(dir, DefaultOSGetter.isA());
		indexedFiles = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx(dir + ".*"));
		assertTrue(indexedFiles.size() > 0);
	}
	
	
	@Test
	public void testListFilesWithRegExpIncludeAndExclude() throws Exception {
		DateTime dateTimeONE = new DateTime().minusHours(1);
		
		indexer.open("one", true, fieldSetId, sourceTags);
		indexer.add("one", 100, dateTimeONE.getMillis(), 100);
		indexer.open("two", true, fieldSetId, sourceTags);
		indexer.add("two", 100, dateTimeONE.getMillis(), 100);
		indexer.open("three-A", true, fieldSetId, sourceTags);
		indexer.add("three-A", 100, dateTimeONE.getMillis(), 100);
		indexer.open("three-B", true, fieldSetId, sourceTags);
		indexer.add("three-B", 100, dateTimeONE.getMillis(), 100);
		
		Collection<LogFile> indexedFiles = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx(".*three.*, !.*A.*"));
		assertEquals(1, indexedFiles.size());
		
	}
	@Test
	public void testListFilesWith2XRegExp() throws Exception {
		DateTime dateTimeONE = new DateTime().minusHours(1);
		
		indexer.open("one", true, fieldSetId, sourceTags);
		indexer.add("one", 100, dateTimeONE.getMillis(), 100);
		indexer.open("two", true, fieldSetId, sourceTags);
		indexer.add("two", 100, dateTimeONE.getMillis(), 100);
		indexer.open("three-A", true, fieldSetId, sourceTags);
		indexer.add("three-A", 100, dateTimeONE.getMillis(), 100);
		indexer.open("three-B", true, fieldSetId, sourceTags);
		indexer.add("three-B", 100, dateTimeONE.getMillis(), 100);
		
		Collection<LogFile> indexedFiles = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx(".*three.*, .*one.*"));
		assertEquals(3, indexedFiles.size());
		
	}
//	@Test
//	public void testListFilesWithIndexAndLimit() throws Exception {
//		disco.open("one", true, isDW, fieldSetId, sourceTags);
//		disco.open("two", true, isDW, fieldSetId, sourceTags);
//		disco.open("three-A", true, isDW, fieldSetId, sourceTags);
//		disco.open("three-B", true, isDW, fieldSetId, sourceTags);
//		disco.open("three-C", true, isDW, fieldSetId, sourceTags);
//
//		Collection<String> indexedFiles = disco.indexedFiles();
//		assertEquals("Got:" + indexedFiles, 6, indexedFiles.size());
//		Collection<LogFile> filter1 = disco.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("three", 100);
//		assertEquals(3, filter1.size());
//
//		Collection<LogFile> filter2 = disco.indexedFiles("", 10);
//		assertEquals(6, filter2.size());
//
//		Collection<LogFile> filter3 = disco.indexedFiles("three", 2);
//		assertEquals(2, filter3.size());
//
//
//		Collection<LogFile> filter4 = disco.indexedFiles("three.*", 2);
//		assertEquals("Got:" + filter4, 2, filter4.size());
//	}
	
//	@Test
//	public void testListFilesWithIndexAndLimitAgain() throws Exception {
//		disco.open("D:\\HPC\\Vscape-agent\\agent\\work\\agent.log", true, isDW, fieldSetId, sourceTags);
//		disco.open("D:\\HPC\\Vscape-agent\\agent\\work\\wrapper\\agent.log", true, isDW, fieldSetId, sourceTags);
//		disco.open("D:\\HPC\\Vscape-agent\\agent\\work\\agent.log.2010-05-05", true, isDW, fieldSetId, sourceTags);
//		disco.open("D:\\HPC\\Vscape-agent\\agent\\work\\agent.log.2010-05-06", true, isDW, fieldSetId, sourceTags);
//		disco.open("D:\\HPC\\Vscape-agent\\agent\\work\\agent.log.2010-05-07", true, isDW, fieldSetId, sourceTags);
//
//		List<LogFile> indexedFiles = disco.indexedFiles(0, Long.MAX_VALUE, false, LogFileOps.FilterCallback.ALWAYS);
//		assertEquals(6, indexedFiles.size());
//
//        List<LogFile> filter3 = disco.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("a"));
//		assertEquals(5, filter3.size());
//
//		Collection<LogFile> filter4 = disco.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("D:\\HPC\\Vscape-agent\\agent\\work\\agent.log"));
//		assertEquals(4, filter4.size());
//
//		Collection<LogFile> filter5 = disco.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("D:\\HPC\\Vscape-agent\\agent\\work\\agent.log"));
//		assertEquals("D:\\HPC\\Vscape-agent\\agent\\work\\agent.log", filter5.iterator().next().getFileName());
//
//		Collection<LogFile> filter6 = disco.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("D:\\HPC\\Vscape-agent\\agent\\work\\agent.log*"));
//		assertEquals(filter6.toString(), 4, filter6.size());
//
//		Collection<LogFile> filter7 = disco.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("*D:\\HPC\\Vscape-agent\\agent\\work\\agent.log*"));
//		assertEquals(4, filter7.size());
//
//		Collection<LogFile> filter8 = disco.indexedFiles(new String[] { "*HPC\\Vscape-agent\\agent\\*\\agent.log" }, -1, 0, Long.MAX_VALUE, true);
//		assertEquals(filter8.size() + " - " + filter8.toString(), 4, filter8.size());
//
//	}
	@Test
	public void testListFilesWithWorkingDir() throws Exception {
		indexer.open("D:\\HPC\\Vscape-agent\\agent\\1work\\agent.log", true, fieldSetId, sourceTags);
		indexer.open("D:\\HPC\\Vscape-agent\\agent\\2work\\wrapper\\agent.log", true, fieldSetId, sourceTags);
		indexer.open("D:\\HPC\\Vscape-agent\\agent\\3work\\agent.log.2010-05-05", true, fieldSetId, sourceTags);
		indexer.open("D:\\HPC\\Vscape-agent\\agent\\4work\\agent.log.2010-05-06", true, fieldSetId, sourceTags);
		indexer.open("D:\\HPC\\Vscape-agent\\agent\\5work\\agent.log.2010-05-07", true, fieldSetId, sourceTags);
		
		List<LogFile> indexedFiles = indexer.indexedFiles(0, Long.MAX_VALUE, false, LogFileOps.FilterCallback.ALWAYS);
		assertEquals(6, indexedFiles.size());
		
		final String fileFilter = FileMaskAdapter.adapt("./agent*", true);
		
		System.out.println("Using fileFilter:" + fileFilter);
		
		Collection<LogFile> filter2 = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx(fileFilter));
		for (LogFile logFile : filter2) {
			System.out.println(">" + logFile.getFileName());
		}
		assertEquals("got:" + filter2, 5, filter2.size());
		
	}
	
	@Test
	public void testFileLineCountAndPos() throws Exception {
		DateTime dateTimeONE = new DateTime().minusHours(1);
		DateTime dateTimeTWO = new DateTime();
		indexer.add("build/myFile.log", 100, dateTimeONE.getMillis(), 100);
		indexer.add("build/myFile.log", 200, dateTimeTWO.getMillis(), 102);
		long[] lastPosAndLastLine = indexer.getLastPosAndLastLine("build/myFile.log");
		
		assertTrue(lastPosAndLastLine[0] > 0);
		assertEquals(200, lastPosAndLastLine[1]);
	}
	@Test
	public void testFileTimes() throws Exception {
		DateTime dateTimeONE = new DateTime().minusHours(1);
		DateTime dateTimeTWO = new DateTime();
		System.out.println("StartTime:" + dateTimeTWO);
		indexer.add("build/myFile4.log", 100, dateTimeONE.getMillis(), 100);
		indexer.add("build/myFile4.log", 200, dateTimeTWO.getMillis(), 102);
		List<DateTime> startAndEndTimes = indexer.getStartAndEndTimes("build/myFile4.log");
		assertTrue(startAndEndTimes.size() == 2);
		assertEquals(dateTimeONE, startAndEndTimes.get(0));
		assertEquals(dateTimeTWO.plusMinutes(1), startAndEndTimes.get(1));
	}
	
	@Test
	public void testIsIndexed() throws Exception {
		assertTrue("should have been true indexed", indexer.isIndexed(LOG_FILE));
		assertFalse(indexer.isIndexed("NoFile"));
	}
	
	@Test
	public void testShouldRollToNewDestination() throws Exception {
		DateTime dateTimeONE = new DateTime().minusMinutes(60);
		indexer.add(LOG_FILE, 100, dateTimeONE.getMillis(), 100);
		List<Bucket> buckets = indexer.find(LOG_FILE, 0, DateTimeUtils.currentTimeMillis());
		
		System.out.println("Buckets:" + buckets);
		assertNotNull(buckets);
		assertTrue(buckets.size() == 1);
		
		List<LogFile> firstIndexedFile = indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx(LOG_FILE));
		LogFile logFile2 = firstIndexedFile.get(0);
		assertTrue(logFile2.getLineCount() > 0);
		
		indexer.rolled(LOG_FILE, "newLog");

		// verify that the existing data is not found against the old name
		assertFalse(indexer.isIndexed(LOG_FILE));
//		List<Bucket> noBuckets = disco.find(LOG_FILE, 0L, DateTimeUtils.currentTimeMillis());
//		assertEquals(0, noBuckets.size());
		
		// verify that the existing data IS_FOUND found against the old name
		assertTrue("should be indexed but isnt!", indexer.isIndexed("newLog"));
		List<Bucket> newBuckets = indexer.find("newLog", 0, DateTimeUtils.currentTimeMillis());
		assertEquals(1, newBuckets.size());
		
		// verify that the file has a line count
		List<LogFile> indexedFiles =  indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx("newLog"));
		LogFile logFile = indexedFiles.get(0);
		assertTrue(logFile.getLineCount() > 0);
	}
	
	@Test
	public void testShouldAddIndexThenRemoveIndexAndBuckets() throws Exception {
		DateTime dateTimeONE = new DateTime().minusMinutes(60);
		indexer.add("build/myFile.log", 100, dateTimeONE.getMillis(), 100);
		List<Bucket> buckets = indexer.find("build/myFile.log", 0L, DateTimeUtils.currentTimeMillis());
		assertNotNull(buckets);
		assertTrue(buckets.size() == 1);
		
		indexer.removeFromIndex("build/myFile.log");
		
		assertFalse(indexer.isIndexed("build/myFile.log"));
//		List<Bucket> noBuckets = disco.find("build/myFile.log", 0L, DateTimeUtils.currentTimeMillis());
//		assertEquals(0, noBuckets.size());
	}
	
	@Test
	public void testShouldAddLINEAndFindBuckets() throws Exception {
		DateTime dateTimeONE = new DateTime().minusMinutes(60);
		indexer.add("build/myFile.log", 100, dateTimeONE.getMillis(), 100);
		List<Bucket> buckets = indexer.find("build/myFile.log", 0L, DateTimeUtils.currentTimeMillis());
		assertNotNull(buckets);
		assertTrue(buckets.size() == 1);
	}
	@Test
	public void testShouldAddLinesAndFindBuckets() throws Exception {
		
		List<Line> lines = new ArrayList<Line>();
		lines.add(new Line(logId, 100, new DateTime().minusMinutes(10).getMillis(), 100));
		lines.add(new Line(logId, 101, new DateTime().minusMinutes(10).getMillis() +1, 101));
		indexer.add(LOG_FILE, lines);
		
		List<Bucket> buckets = indexer.find(LOG_FILE, new DateTime().minusHours(60).getMillis(), DateTimeUtils.currentTimeMillis());
		assertNotNull(buckets);
		assertEquals(1, buckets.size());
		
		List<LogFile> indexedFiles =  indexer.indexedFiles(0, Long.MAX_VALUE, false, new LogFileOps.FilterCallbackRegEx(LOG_FILE));
		assertTrue(1 == indexedFiles.size());
		
		assertTrue(indexedFiles.get(0).getPos() > 0);

        indexer.removeFromIndex(LOG_FILE);
        assertFalse(indexer.isIndexed(LOG_FILE));
		
	}
	
	@Test
	public void testShouldOpenALogAndListContents() throws Exception {
		indexer.open("myLog1", true, fieldSetId, sourceTags);
		indexer.open("myLog2", true, fieldSetId, sourceTags);
		indexer.open("myLog3", true, fieldSetId, sourceTags);
        final Set<String> files = getAllFilenames();


        assertTrue(files.contains("myLog1"));
		assertTrue(files.contains("myLog2"));
		assertTrue(files.contains("myLog3"));
	}

    private Set<String> getAllFilenames() {
        final Set<String> files = new HashSet<String>();
        indexer.indexedFiles(new LogFileOps.FilterCallback() {
            public boolean accept(LogFile logFile) {
                files.add(logFile.getFileName());
                return false;
            }
        });
        return files;
    }


    @Test
	public void testShouldInsertAndGetLinesForTime() throws Exception {
		DateTime dateTimeONE = new DateTime().minusMinutes(60);
		dateTimeONE = dateTimeONE.minusSeconds(dateTimeONE.getSecondOfMinute());
		dateTimeONE = dateTimeONE.minusMillis(dateTimeONE.getMillisOfSecond());
		
		System.out.println("DateTime:" + DateTimeFormat.longTime().print(dateTimeONE));

		String LOG_FILE = "build/myFile6.log";
		
		long millis00 = dateTimeONE.minusMinutes(1).getMillis();
		
		for (int i = 0; i < 10; i++) {
			indexer.add(LOG_FILE, i, millis00, i);
		}
		for (int i = 10; i < 20; i++) {
			indexer.add(LOG_FILE, i, dateTimeONE.getMillis(), i);
		}
		for (int i = 20; i < 30; i++) {
			indexer.add(LOG_FILE, i, dateTimeONE.plusMinutes(1).getMillis(), i);
		}
		
		DateTimeFormatter formatter = DateTimeFormat.longTime();
		List<Line> linesForNumbers = indexer.linesForTime(LOG_FILE, dateTimeONE.getMillis(), 100);
		
		assertEquals(20, linesForNumbers.size());
		for (int i = 0; i < 10; i++) {
			Line line = linesForNumbers.get(i);
			long time = line.time();
			System.out.println("Processing item:" + i + " myTime:" + formatter.print(millis00) + " itsTime:" + formatter.print(time) + " line:" + line.number());
			assertEquals("Got:" + DateTimeFormat.longTime().print( linesForNumbers.get(i).time()), dateTimeONE.getMillis(), time);
		}
		for (int i = 10; i < 20; i++) {
			System.out.println("Processing item:" + i);
			assertEquals(dateTimeONE.plusMinutes(1).getMillis(), linesForNumbers.get(i).time());
		}
		indexer.removeFromIndex(LOG_FILE);
	}
	
	
	@Test
	public void testShouldGetSortedFilesCorrectly() throws Exception {
		indexer.removeFromIndex(LOG_FILE);
		
		indexer.open("aaa",	true, fieldSetId, sourceTags);
		indexer.open("bbb",	true, fieldSetId, sourceTags);
		indexer.open("ccc",	true, fieldSetId, sourceTags);
		
		DateTime dateTime = new DateTime().minusMinutes(60);
		dateTime = dateTime.minusSeconds(dateTime.getSecondOfMinute());
		dateTime = dateTime.minusMillis(dateTime.getMillisOfSecond());
		
		System.out.println("DateTime:" + DateTimeFormat.longTime().print(dateTime));
		
		for (int i = 0; i < 10; i++) {
			indexer.add("aaa", i, dateTime.getMillis(), i);
		}
		for (int i = 10; i < 20; i++) {
			indexer.add("bbb", i, dateTime.minusMinutes(2).getMillis(), i);
		}
		for (int i = 20; i < 30; i++) {
			indexer.add("ccc", i, dateTime.plusMinutes(2).getMillis(), i);
		}
        List<LogFile> files = indexer.indexedFiles(0, Long.MAX_VALUE, true, new LogFileOps.FilterCallback() {
			@Override
			public boolean accept(LogFile logFile) {
				String f = logFile.getFileNameOnly();
				return f.contains("aaa") || f.contains("bbb") || f.contains("ccc");
			}
		});

        for (LogFile file : files) {
            System.out.println(file);
        }

        assertTrue(files.size() > 0);
		assertEquals("ccc", files.get(0).getFileName());
		assertEquals("aaa", files.get(1).getFileName());
		assertEquals("bbb", files.get(2).getFileName());
	}
	
	@Test
	public void testShouldInsertAndGetLines() throws Exception {
		DateTime baseTimee = new DateTime();
		baseTimee = baseTimee.minusSeconds(baseTimee.getSecondOfMinute());
		baseTimee = baseTimee.minusMillis(baseTimee.getMillisOfSecond());
		
		System.out.println("DateTime:" + DateTimeFormat.longTime().print(baseTimee));
		
		
		
		for (int i = 0; i < 10; i++) {
			System.out.println("Added:" + i + " t:" + baseTimee.minusMinutes(2));
			indexer.add(LOG_FILE, i, baseTimee.minusMinutes(2).getMillis(), i);
		}
		for (int i = 10; i < 20; i++) {
			indexer.add(LOG_FILE, i, baseTimee.minusMinutes(1).getMillis(), i);
		}
		for (int i = 20; i < 30; i++) {
			indexer.add(LOG_FILE, i, baseTimee.minusMinutes(0).getMillis(), i);
		}
		
		DateTimeFormatter formatter = DateTimeFormat.longTime();
		List<Line> linesForNumbers = indexer.linesForNumbers(LOG_FILE, 5, 25);
		
		assertTrue(linesForNumbers.size() > 0);
		
		System.out.println("0-9\t" + baseTimee.minusMinutes(2).getMillis());
		System.out.println("10-19\t" + baseTimee.minusMinutes(1).getMillis());
		System.out.println("20-30\t" + baseTimee.minusMinutes(0).getMillis());
		
		for (int i = 0; i < 10; i++) {
			Line line = linesForNumbers.get(i);
			long time = line.time();
			System.out.println("aProcessing item:" + i + " myTime:" +  baseTimee.minusMinutes(2)+ " itsTime:" + formatter.print(time) + " line:" + line.number());
			assertEquals("Line:" + i + " Got:" + DateTimeFormat.longTime().print( linesForNumbers.get(i).time()),  baseTimee.minusMinutes(2).getMillis(), time);
		}
		for (int i = 10; i < 19; i++) {
			System.out.println("bProcessing item:" + i + " Line:" + linesForNumbers.get(i).toString());
			assertEquals("FailedItem:" + i,new DateTime( baseTimee.minusMinutes(1).getMillis()), new DateTime(linesForNumbers.get(i).time()));
		}
	}
//
//	@Test
//	public void testShouldIndexLinesAddPatternAndExtractIsAllCorrectly() throws Exception {
//		DateTime dateTimeONE = new DateTime();
//		List<Line> lines = new ArrayList<Line>();
//		for (int i = 0; i < 10; i++) {
//			lines.add(new Line(LOG_FILE.hashCode(), i, dateTimeONE.getMillis()+i, i));
//		}
//
//		// add the lines to the bucket
//		disco.add(LOG_FILE, lines);
//
//		DateTime specialTime = dateTimeONE.minusMillis(dateTimeONE.getMillisOfSecond());
//		specialTime = specialTime.minusSeconds(specialTime.getSecondOfMinute());
//
//		// add a bunch of lines to a pattern 'stuff' - make sure we store the line content
//		Map<PatternBucketKey, List<Line>> indexedLinesWithAccurateTime = new HashMap<PatternBucketKey, List<Line>>();
//		PatternBucketKey qbKey = new PatternBucketKey(logId, fieldSetId, "data", "stuff", dateTimeONE.getMillis());
//		List<Line> subList = lines.subList(3, 7);
//		for (Line line : subList) {
//			// mess up the time by 999 ms - should be rectified to nearest minute
//			line.setTime(specialTime.getMillis() + 999);
//		}
//		indexedLinesWithAccurateTime.put(qbKey, subList);
//		disco.addPatternIndexHits(logId, indexedLinesWithAccurateTime);
//
//		// now see if we can re-assemble it
//		List<Line> newLines = disco.linesForTime(LOG_FILE, dateTimeONE.getMillis(), 300);
//		assertTrue("Newlines was not > 0 - was:" + newLines, newLines.size() > 0);
//
//
//		Set<String> noPatterns = disco.patterns("doesntExist");
//		assertEquals(0, noPatterns.size());
//
//		Set<String> patterns = disco.patterns(LOG_FILE);
//		assertNotNull(patterns);
//		assertEquals(1, patterns.size());
//		assertTrue(patterns.toString(), patterns.toString().contains("stuff"));
//
//		List<Line> updatedLines = disco.refineLineTimesAgainstIndexData(logId, fieldSetId, patterns, newLines);
//		assertNotNull(updatedLines);
//		assertTrue("UpdatedLines should be > 0", updatedLines.size() > 0);
//		// check the special lines were refined using the index time
//		assertEquals("UpdatedLines:" + updatedLines.get(3), specialTime.getMillis() + 999, updatedLines.get(3).time());
//
//	}

	@Test
	public void shouldReclassifyFile() throws Exception {
		String testFile = new File("test-data/agent.log").getAbsolutePath();
        LogFile file = indexer.openLogFile(testFile, true, "basic", sourceTags);

        assertEquals("basic", file.fieldSetId.toString());

        indexer.add(testFile, 100, System.currentTimeMillis() - DateUtil.HOUR, 100);
        file = indexer.openLogFile(testFile, true, "basic", sourceTags);
		FieldSet log4JFieldSet = FieldSets.getLog4JFieldSet();

        assertNotNull("didnt store file", indexer.openLogFile(testFile));
        assertTrue(indexer.isIndexed(testFile));

        log4JFieldSet.filePathMask = "**";
		indexer.addFieldSet(log4JFieldSet);

        file = indexer.openLogFile(testFile, true, "basic", sourceTags);
		
		// check it was applied
		LogFile openLogFile = indexer.openLogFile(testFile);
		assertEquals(log4JFieldSet.id, openLogFile.fieldSetId.toString());
		
		// now override log4j - with log4j2
		FieldSet log4JFieldSet2 = FieldSets.getLog4JFieldSet();
		log4JFieldSet2.id = "log4j2";
        log4JFieldSet2.filePathMask = "**";
		log4JFieldSet2.priority = log4JFieldSet.priority + 10;
		indexer.addFieldSet(log4JFieldSet2);
		
		// check it was applied
		LogFile openLogFile2 = indexer.openLogFile(testFile);
		assertEquals(log4JFieldSet2.id, openLogFile2.fieldSetId.toString());
	}
	
	@Test
	public void shouldAddLogFileWithFieldSet() throws Exception {
		String testFile = "build/bigIndexerFieldSetTest.log";
		FileOutputStream fos = new FileOutputStream(testFile);
		fos.write("line 1\n".getBytes());
		fos.write("line 2\n".getBytes());
		fos.write("line 3\n".getBytes());
		fos.close();
		indexer.openLogFile(testFile, true, "basic", sourceTags);
		FieldSet newFieldSet = new FieldSet("(**)","stuff");
        newFieldSet.id = "new";
        newFieldSet.filePathMask = "**";
        newFieldSet.priority = 10;
		indexer.addFieldSet(newFieldSet);
		LogFile updated = indexer.openLogFile(testFile);
		assertEquals("new", updated.getFieldSetId());
		FieldSet fieldSet2 = indexer.getFieldSet(newFieldSet.id);
		assertNotNull(fieldSet2);
		
		
	}
	@Test
	public void shouldRevertFieldSetToBasic() throws Exception {
		String testFile = "build/bigIndexerFieldSetTest3.log";
		FileOutputStream fos = new FileOutputStream(testFile);
		fos.write("line 1\n".getBytes());
		fos.write("line 2\n".getBytes());
		fos.write("line 3\n".getBytes());
		fos.close();
		indexer.openLogFile(testFile, true, "new", sourceTags);
		//FieldSet newFieldSet = new FieldSet("new", new String[0], "(**)", "*", 10, new String[] { "stuff" }, new String [] { "count()" });
        FieldSet newFieldSet = new FieldSet("(**)","stuff");
		indexer.addFieldSet(newFieldSet);
		LogFile updated = indexer.openLogFile(testFile);
		assertEquals("new", updated.getFieldSetId());
		
		// now remove and check that it was reverted to basic
		indexer.removeFieldSet(newFieldSet);
		LogFile updated2 = indexer.openLogFile(testFile);
		assertEquals("basic", updated2.getFieldSetId());
	}
}
