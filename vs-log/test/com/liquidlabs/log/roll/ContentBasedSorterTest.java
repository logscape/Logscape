package com.liquidlabs.log.roll;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.LogProperties;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.*;

public class ContentBasedSorterTest {


    private ContentBasedSorter sorter;
    String dir = "build/contentSortTest/" + new DateTime().getSecondOfMinute();
    String timeFormat = LogProperties.FORMATS[0];
    private boolean verbose;
    private String firstFileLine;

    @Before
    public void setUp() throws Exception {
        FileUtil.deleteDir(new File(dir));

        sorter = new ContentBasedSorter();

        FileUtil.mkdir(dir);
    }

    @Test
    public void shouldDetectNumericRoll() throws Exception {
        String filename = "/logs01/WebSphere85/BPMTSTnode01/TSTCELL01DE4.AppTarget01/SystemOut_16.09.14_00.03.54.log";
        String goodMatchOne = "opendirectoryd.log.0";
        String goodMatchTwo = "opendirectoryd.0.log";
        String badMatch = "opendirectoryd.0.0.log";

        assertFalse(ContentBasedSorter.isNumericalRollFile(filename));
        assertTrue(ContentBasedSorter.isNumericalRollFile(goodMatchOne));
        assertTrue(ContentBasedSorter.isNumericalRollFile(goodMatchTwo));
        assertFalse(ContentBasedSorter.isNumericalRollFile(badMatch));
    }


    @Test
    public void shouldRecogniseNumericalRoll() throws Exception {
        String file1 = "agent.log.1";
        String file2 = "agent.log";
        assertTrue(sorter.isNumericalRollFile(file1));
        assertFalse(sorter.isNumericalRollFile(file2));

    }

    @Test
    public void shouldRollNumericallyGZ() throws Exception {
        String file1 = "agent.log.1.gz";
        File file = new File(dir, file1);
        OutputStream fos = new GZIPOutputStream(new FileOutputStream(file));
        fos.write("crap\n".getBytes());
        fos.write("crap\n".getBytes());
        fos.close();


        List<String> matchingNames = sorter.getMatchingNames(dir, "agent.log", Arrays.asList("agent.log.1.gz"), ".", file1.length()-2, verbose, new HashSet<String>(), "crap");
        assertTrue(matchingNames.size() == 1);
    }

    @Test
    public void shouldRollNumerically() throws Exception {
        String file1 = "agent.log.1";
        File file = new File(dir, file1);
        FileOutputStream fos = new FileOutputStream(file);
        fos.write("crap\n".getBytes());
        fos.write("crap\n".getBytes());
        fos.close();


        List<String> matchingNames = sorter.getMatchingNames(dir, "agent.log", Arrays.asList("agent.log.1"), ".", file1.length()-2, verbose, new HashSet<String>(), "crap");
        assertTrue(matchingNames.size() == 1);
    }

    @Test
    public void shouldWorkWithShortnames() throws Exception {
        String file1 = "agent.log.2";
        File file = new File(dir, file1);
        FileOutputStream fos = new FileOutputStream(file);
        fos.write("crap\n".getBytes());
        fos.write("crap\n".getBytes());
        fos.close();


        List<String> matchingNames = sorter.getMatchingNames(dir, "agent", Arrays.asList("agent.log.2"), ".", file1.length() - 2, verbose, new HashSet<String>(), "crap");
        assertTrue(matchingNames.size() == 1);


    }



    @Test
    public void shouldGetRightRollNames() throws Exception {
        String[] filenames =  "agent.log,agent-spawned.log,boot_rs.log,boot_wa.log,logspace.log,dashboard.log,dashboard-jetty.log,agent-spawned.log.2010-01-04,agent.log.2010-01-04,dashboard.log.2010-01-04,boot_rs.log.2010-01-04,logspace.log.2010-01-04,agent.log.2010-01-02,boot_rs.log.2010-01-02,dashboard.log.2010-01-02,logspace.log.2010-01-02,agent.log.2010-01-03,boot_rs.log.2010-01-03,dashboard.log.2010-01-03,agent-spawned.log.2010-01-03,logspace.log.2010-01-03".split(",");
        List<File> files = new ArrayList<File>();
        List<String> rollToNames = new ArrayList<String>();
        for (String string : filenames) {
            File file = new File(dir, string);
            FileOutputStream fos = new FileOutputStream(file);
            fos.write("crap\n".getBytes());
            fos.close();
            files.add(file);
            rollToNames.add(string);
        }

        firstFileLine = "crap";
        String[] matchingNames = sorter.sortedFileNames(false, files.get(0).getAbsolutePath(), files.get(0).getName(), files.get(0).getParentFile().getName(), files.get(0).getParentFile().getAbsolutePath(), new HashSet<String>(), files.get(0).length(), verbose, firstFileLine);
        assertEquals("Got:" + Arrays.toString(matchingNames), 1, matchingNames.length);
    }

    @Test
    public void testShouldAccountNotRollToShorterFilename() throws Exception {
        File file = new File(dir, "roll-agent.log.100");
        FileOutputStream fos = new FileOutputStream(file);
        fos.write("stuff\n".getBytes());
        fos.close();
        Thread.sleep(1000);

        FileOutputStream fos2 = new FileOutputStream(new File(dir, "roll-agent.log.200"));
        fos2.write("stuff\n".getBytes());
        fos2.close();
        Thread.sleep(1000);

        FileOutputStream fos3 = new FileOutputStream(new File(dir, "roll-agent.log.300"));
        fos3.write("stuff\n".getBytes());
        fos3.close();
        Thread.sleep(1000);

        firstFileLine = "stuff";
        List<String> matchingNames = sorter.getMatchingNames(dir, "roll-agent.log", Arrays.asList("roll-agent.log.100", "roll-agent.log.300", "roll-agent.log.200"), ".", file.length(), verbose, new HashSet<String>(), firstFileLine);
        assertEquals(3, matchingNames.size());
        assertEquals("roll-agent.log.100", matchingNames.get(0));

    }

    @Test
    public void testShouldNotRollToShorterFilename() throws Exception {
        String rollFrom = "roll-agent.log.one";
        File file = new File(dir, rollFrom);
        FileOutputStream fos = new FileOutputStream(file);
        firstFileLine = "stuff";
        fos.write(firstFileLine.getBytes());
        fos.close();

        FileOutputStream fos2 = new FileOutputStream(new File(dir, "roll-agent.log"));
        fos2.write(firstFileLine.getBytes());
        fos2.close();

        FileOutputStream fos3 = new FileOutputStream(new File(dir, "roll-agent.log.one.yes"));
        fos3.write(firstFileLine.getBytes());
        fos3.close();

        List<String> matchingNames = sorter.getMatchingNames(dir, rollFrom, Arrays.asList("roll-agent.log", "roll-agent.log.one.yes"), ".", file.length(), verbose, new HashSet<String>(), firstFileLine);
        assertEquals(1, matchingNames.size());
        assertEquals("roll-agent.log.one.yes", matchingNames.get(0));
    }

    @Test
    public void testShouldFindSuitableNamesForMsRolling() throws Exception {

//        cdcs192501-1.log becomes cdcs192510-1-19-10-14.log
        String[] files = new String[] { "CDCS192510-20_20141014.log", "CDCS192510-20_20141015.log"};
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timeFormat);
        String line = String.format("%s SomeLineOfCrap\n", simpleDateFormat.format(new Date()));
        firstFileLine = line;
        for (String string : files) {
            if (string.endsWith(".dir")) {
                new File(dir, string).mkdir();
                continue;
            }
            File file = new File(dir, string);
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(line.getBytes());
            fos.close();
        }

//        sorter.setFilenamePatterns(new String[] { "agent.log.*" });
        List<String> filteredFiles = sorter.filterFiles("CDCS192510-20.log", dir);
        List<String> matchingNames = sorter.getMatchingNames(dir, "CDCS192510-20.log", filteredFiles, ".", 1, verbose, new HashSet<String>(), firstFileLine);
        assertEquals(dir + " Got:" + matchingNames.size(), 2, matchingNames.size());

    }

    @Test
    public void testShouldFind_AGEON_SuitableNames() throws Exception {

        String[] files = new String[] { "trace_16.09.23_09.54.15.log" ,"trace_16.09.23_09.55.15.log","trace_16.09.23_09.56.15.log"  };
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timeFormat);
        String line = String.format("%s SomeLineOfCrap\n", simpleDateFormat.format(new Date()));
        firstFileLine = line;
        for (String string : files) {
            File file = new File(dir, string);
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(line.getBytes());
            fos.close();
        }

        //String[] patterns = "trace.*, .*trace.*".split(",");
        String[] patterns = "trace.*.log".split(",");
        sorter.setFilenamePatterns(patterns);
        List<String> filteredFiles = sorter.filterFiles("trace.log", dir);
        assertTrue(filteredFiles.size() > 0);
//        List<String> matchingNames = sorter.getMatchingNames(dir, "trace.log", filteredFiles, ".", 1, verbose, new HashSet<String>(), firstFileLine);
//        assertEquals(dir + " Got:" + matchingNames.size(), 3, matchingNames.size());

    }

    @Test
    public void testShouldFindSuitableNames() throws Exception {

        String[] files = new String[] { "agent-spawned.log" ,"agent.log.2009-04-17-17-53", "agent.log.2009-04-17-17-54", "agent.log.2009-04-17-17-55","agent.log.dir"};
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timeFormat);
        String line = String.format("%s SomeLineOfCrap\n", simpleDateFormat.format(new Date()));
        firstFileLine = line;
        for (String string : files) {
            if (string.endsWith(".dir")) {
                new File(dir, string).mkdir();
                continue;
            }
            File file = new File(dir, string);
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(line.getBytes());
            fos.close();
        }

        sorter.setFilenamePatterns(new String[] { "agent.log.*" });
        List<String> filteredFiles = sorter.filterFiles("agent.log", dir);
        List<String> matchingNames = sorter.getMatchingNames(dir, "agent.log", filteredFiles, ".", 1, verbose, new HashSet<String>(), firstFileLine);
        assertEquals(dir + " Got:" + matchingNames.size(), 3, matchingNames.size());

    }
    @Test
    public void testShouldFindSuitableNamesNumeric() throws Exception {

        String[] files = new String[] { "agent-spawned.log" ,"agent.log.1", "agent.log.2", "agent.log.3"};
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timeFormat);
        String line = String.format("%s SomeLineOfCrap\n", simpleDateFormat.format(new Date()));
        firstFileLine = line;
        for (String string : files) {
            if (string.endsWith(".dir")) {
                new File(dir, string).mkdir();
                continue;
            }
            File file = new File(dir, string);
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(line.getBytes());
            fos.close();
        }

        sorter.setFilenamePatterns(new String[]{"agent.log.*"});
        List<String> filteredFiles = sorter.filterFiles("agent.log", dir);
        List<String> matchingNames = sorter.getMatchingNames(dir, "agent.log", filteredFiles, ".", 1, verbose, new HashSet<String>(), firstFileLine);
        assertEquals(dir + " Got:" + matchingNames.size(), 3, matchingNames.size());

    }

    @Test
    public void testShouldFindRightFilenamesInFDir() throws Exception {
        FileOutputStream fos = new FileOutputStream(dir + "/agent.log.1");
        String line = "2009-03-19 14:40:01,896 INFO tailer-8-26 (SpaceWriter.java:237)\n";
        fos.write(line.getBytes());
        fos.close();

        fos = new FileOutputStream(dir + "/agent.log.2");
        line = "2009-03-19 14:50:01,896 INFO tailer-8-26 (SpaceWriter.java:237)\n";
        // ************ to this file!
        firstFileLine = line;
        fos.write(line.getBytes());
        fos.close();

        fos = new FileOutputStream(dir + "/agent.log");
        line = "2009-03-19 15:40:01,896 INFO tailer-8-26 (SpaceWriter.java:237)\n";
        fos.write(line.getBytes());
        fos.close();

        System.out.println("lastmod:" + new File(dir + "/agent.log.2").exists() + " lm:" + new File(dir + "/agent.log.2").lastModified());
        File file = new File(dir + "/agent.log");


        String[] sortedFileNames = sorter.sortedFileNames(false, file.getAbsolutePath(), file.getName(), file.getParent(), file.getParentFile().getAbsolutePath(), new HashSet<String>(), file.length(), verbose, firstFileLine);
        assertEquals(1, sortedFileNames.length);
        assertTrue("Got:" + sortedFileNames[0], sortedFileNames[0].contains("agent.log.2"));
    }

    @Test
    public void testShouldGiveGoodTimeFromFile() throws Exception {
        String filename = "build/contentsortertest.log";
        FileOutputStream fos = new FileOutputStream(filename);
        fos.write("crap\n".getBytes());
        String line = "2009-03-19 14:40:01,896 INFO tailer-8-26 (SpaceWriter.java:237)  - LOGGER TAILER - failed parseFile[agent-spawned.log:151] timeFormat[null] line[14:40:00,197 INFO PFstcp://alteredcarbon.local:10000-4-11 (AggSpaceImpl.java:190)      - AggSpace alteredcarbon.local CPU:9] UsingTime2009-03-19T14:39:58.819Z\n";
        fos.write(line.getBytes());
        String line2 = "2009-03-19 14:41:01,896 INFO tailer-8-26 (SpaceWriter.java:237)  - LOGGER TAILER - failed parseFile[agent-spawned.log:151] timeFormat[null] line[14:40:00,197 INFO PFstcp://alteredcarbon.local:10000-4-11 (AggSpaceImpl.java:190)      - AggSpace alteredcarbon.local CPU:9] UsingTime2009-03-19T14:39:58.819Z\n";
        fos.write(line2.getBytes());
        fos.close();
        long timeFromFile = sorter.getTimeFromFile(".", filename);

        assertTrue(timeFromFile > 0);
        assertTrue(new DateTime(timeFromFile).getYear() > 2008);
    }
}
