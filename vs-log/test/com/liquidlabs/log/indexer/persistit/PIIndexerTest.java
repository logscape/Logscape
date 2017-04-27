package com.liquidlabs.log.indexer.persistit;

import com.liquidlabs.common.UID;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.*;
import com.liquidlabs.log.indexer.persistit.PIIndexer;
import com.logscape.disco.indexer.KvIndexFeed;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 09:15
 * To change this template use File | Settings | File Templates.
 */
public class PIIndexerTest {

    private static final String LOG_FILE = "build/myFile" + System.currentTimeMillis() + ".log";
    protected String DIR = "build/PIIndexerTest" + System.currentTimeMillis() ;
    private FieldSet fieldSet;
    protected Indexer indexer;
    private String fieldSetId;
    private String sourceTags = "";
    static int count = 0;
    protected KvIndexFeed kvIndexFeed;

    @Before
    public void setUp() throws Exception {
        try {

            DIR = DIR + count++;
            fieldSet = new FieldSet("(**)","data");
            fieldSet.id = UID.getUUID();
            fieldSetId = fieldSet.id;

            FileUtil.deleteDir(new File(DIR));
            kvIndexFeed = mock(KvIndexFeed.class);
            getIndexer();

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    protected void getIndexer() {
        indexer = new PIIndexer(DIR, kvIndexFeed);
    }

    @After
    public void tearDown() throws Exception {
        if (indexer == null) {
            System.err.println("Indexer is null!");
        } else {
//            indexer.close();

            System.gc();
        }
//        FileUtil.deleteDir(new File(DIR));


    }

//    @Test
//    public void testLineStorePutGet() throws Exception {
//        LineStore lineStore = indexer.lineStore();
//        BucketKey key = new BucketKey(11, 100);
//        lineStore.put(key, new Bucket(10,1001));
//
//        Bucket bucket = lineStore.get(key);
//        assertNotNull("Didnt load a bucket", bucket);
//
//    }

    @Test
    public void testShouldWaitTenSeconds() throws Exception {
        PIIndexer idx = (PIIndexer) indexer;
        assertTrue(idx.isReadyForNextSync());
        idx.lastSync = new DateTime().minusSeconds(1).getMillis();
        assertFalse(idx.isReadyForNextSync());
        idx.lastSync = new DateTime().minusSeconds(11).getMillis();
        assertTrue(idx.isReadyForNextSync());


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
        if (indexer.isIndexed(LOG_FILE)) indexer.removeFromIndex(LOG_FILE);
        DateTime dateTimeONE = new DateTime().minusHours(1);
        DateTime dateTimeTWO = new DateTime();
        System.out.println("StartTime:" + dateTimeTWO);
        indexer.add(LOG_FILE, 100, dateTimeONE.getMillis(), 100);
        indexer.add(LOG_FILE, 200, dateTimeTWO.getMillis(), 102);
        List<DateTime> startAndEndTimes = indexer.getStartAndEndTimes(LOG_FILE);
        assertTrue(startAndEndTimes.size() == 2);
        assertEquals(dateTimeONE, startAndEndTimes.get(0));
        assertEquals(dateTimeTWO.plusMinutes(1), startAndEndTimes.get(1));
    }

    @Test
    public void shouldDeleteFromKvIndex() {
        indexer.add(LOG_FILE, 1,1,1);
        indexer.removeFromIndex(LOG_FILE);
        Mockito.verify(kvIndexFeed).remove(anyInt());
    }

    @Test
    public void testIsIndexed() throws Exception  {
        indexer.open("build/isIndexed.log", true, fieldSetId, sourceTags );
        indexer.open("build/isIndexed.log", true, fieldSetId, sourceTags );
        assertTrue(indexer.isIndexed("build/isIndexed.log"));
        assertFalse(indexer.isIndexed("NoFile"));
    }

    @Test
    public void shouldOpenAndAppendToLogFile() throws Exception {
        String filename = "build/appendFile";
        LogFile logFile1 = indexer.openLogFile(filename, true, "basic", sourceTags);
        assertNotNull(logFile1);
        assertTrue(indexer.isIndexed(filename));

        int line = 1;
        long time = System.currentTimeMillis() - 60 * 1000;
        long pos = 1000;

        for (int i = 0; i < 100; i++) {
            indexer.add(filename, line, time, pos);
            line++;
            time += 1000;
            pos += 10 * 1000;
        }

    }

    @Test
    public void shouldGenerateUniqueIdsForFiles() throws Exception {
        LogFile logFile1 = indexer.openLogFile("build/testFile1", true, "basic", sourceTags);
        assertNotNull(logFile1);

        LogFile logFile2 = indexer.openLogFile("build/testFile2", true, "basic", sourceTags);
        assertNotNull(logFile2);

        assertTrue("id:" + logFile1.getId() + " 2id:" + logFile2.getId(), logFile1.getId() != logFile2.getId());



    }

    @Test
    public void shouldOpenAndFindExistingLogFile() throws Exception {
        String testFile = "build/bigIndexerFieldSetTest.log";
        LogFile logFile1 = indexer.openLogFile(testFile, true, "basic", sourceTags);
        assertNotNull(logFile1);

        LogFile logFile2 = indexer.openLogFile(testFile);
        assertNotNull(logFile1);
    }
    @Test
    public void shouldLoadStdFieldSet() throws Exception {
        indexer.addFieldSet(FieldSets.getLog4JFieldSet());
        FieldSet fieldSet1 = indexer.getFieldSet(FieldSets.getLog4JFieldSet().getId());

        FieldSet basic = indexer.getFieldSet("basic");
        assertNotNull(basic);
        List<FieldSet> list = indexer.getFieldSets(new Indexer.Filter<FieldSet>() {
            @Override
            public boolean accept(FieldSet thing) {
                return true;
            }
        });
        assertTrue(list.size() > 0);
    }


    @Test
    public void testShouldInsertAndGetLines() throws Exception {
        if (indexer.isIndexed(LOG_FILE)) indexer.removeFromIndex(LOG_FILE);
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

        assertTrue("Got No Lines!!", linesForNumbers.size() > 0);

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
    @Test
    public void testShouldInsertAndGetLinesForTime() throws Exception {
        if (indexer.isIndexed(LOG_FILE)) indexer.removeFromIndex(LOG_FILE);
        System.out.println("LogFIile:" + LOG_FILE);
        DateTime dateTimeONE = new DateTime().minusMinutes(60);
        dateTimeONE = dateTimeONE.minusSeconds(dateTimeONE.getSecondOfMinute());
        dateTimeONE = dateTimeONE.minusMillis(dateTimeONE.getMillisOfSecond());

        System.out.println("DateTime:" + DateTimeFormat.longTime().print(dateTimeONE));

        System.out.println("0-Lines:" + indexer.linesForTime(LOG_FILE, dateTimeONE.getMillis(), 100).size());

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
    public void shouldReclassifyFile() throws Exception {
        if (indexer.isIndexed(LOG_FILE)) indexer.removeFromIndex(LOG_FILE);
        // remove log4j2
        //disco.removeFieldSet(fieldSet);
        String testFile = new File("test-data/agent2.log").getAbsolutePath();
        indexer.openLogFile(testFile, true, "basic", sourceTags);
        FieldSet log4JFieldSet = FieldSets.getLog4JFieldSet();
        log4JFieldSet.filePathMask += ",./test-data";
        indexer.addFieldSet(log4JFieldSet);

        // check it was applied
        LogFile openLogFile = indexer.openLogFile(testFile);
        assertEquals(log4JFieldSet.id, openLogFile.fieldSetId.toString());

        // now override log4j - with log4j2
        FieldSet log4JFieldSet2 = FieldSets.getLog4JFieldSet();
        log4JFieldSet2.filePathMask += ",./test-data";
        log4JFieldSet2.id = "log4j2";
        log4JFieldSet2.priority = log4JFieldSet.priority + 10;
        indexer.addFieldSet(log4JFieldSet2);

        // check it was applied
        LogFile openLogFile2 = indexer.openLogFile(testFile);
        assertEquals(log4JFieldSet2.id, openLogFile2.fieldSetId.toString());
    }


}
