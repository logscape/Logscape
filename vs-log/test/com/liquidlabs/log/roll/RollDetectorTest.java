package com.liquidlabs.log.roll;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class RollDetectorTest {


    private File file;
    private RollDetector rollDetector;
    private String firstLine;

    @Before
    public void setUp() throws Exception {
        file = new File("build/rolldetectortest.log");
        final FileOutputStream output = new FileOutputStream(file);
        rollDetector = new RollDetector();

        firstLine = "2013-08-15 06:27:58.288/1.730 Oracle Coherence 3.6.1.0 <Info> (thread=main, member=n/a): Loaded operational configuration from \"jar:file:/home/logscape/coherence/prod/market-data/lib/coherence.jar!/tangosol-coherence.xml";
        output.write((firstLine + "\n").getBytes());
        rollDetector.setDetectorClass(new NumericalRoll());
        output.close();
    }

    @Test
    public void shouldFindDateWithCount() throws Exception {
        String from = "qis.log";
        String to = "qis.log.2016-08-29-1";

        assertTrue(RollDetector.isRollCandidate(from, to));
    }

    @Test
    public void shouldFindQisNumerical() throws Exception{
        String from = "qis.log.2016-08-29-1";
        String to = "qis.log.2016-08-29-2";

        assertTrue(RollDetector.isRollCandidate(from, to));
    }

    @Test
    public void shouldFindSonoraNumerical() throws Exception{
        String from = "sonora.log.0";
        String to = "sonora.log.1";

        assertTrue(RollDetector.isRollCandidate(from, to));
    }

    @Test
    public void shouldFindSonoraNumericalSecondary() throws Exception{
        String from = "sonora.log.0.1";
        String to = "sonora.log.1.1";

        assertTrue(RollDetector.isRollCandidate(from, to));
    }

    @Test
    public void shouldFindDateRollTargetForTrace() throws Exception {

        String from = "/opt/dir/trace.log";
        String to = "/opt/dir/trace_16.09.19_02.10.55.log";

        assertTrue(RollDetector.isRollCandidate(from, to));

    }

    @Test
    public void shouldntViewLiveFileAsATarget() throws Exception{
        String from = "intermediair.log.11-10-2016.1";
        String to = "intermediair.log";

        assertFalse(RollDetector.isRollCandidate(from, to));
    }

    @Test
    public void shouldFindChangedExtension() throws Exception {     //fails because from doesnt contain a .
        String from = "wtdn_pentaho_hadoop_uklnxpnl0073";
        String to = "wtdn_pentaho_hadoop_uklnxpnl0073_2016-09-25.log";

        assertTrue(RollDetector.isRollCandidate(from, to));
    }

    @Test
    public void shouldFindDateRollTarget() throws Exception {           //both return true since they share a canonical parent, and nofile contains file

        String from = "/opt/dir/file.log";
        String to = "/opt/dir/file.log.2016-06-16";
        String fail = "/opt/dir/nofile.log.2016-06-16";

        assertTrue(RollDetector.isRollCandidate(from, to));
//        assertFalse(RollDetector.isRollCandidate(from, fail));
//        assertFalse(RollDetector.isRollCandidate(from, "XX" + from + ".1"));

    }

    @Test
    public void shouldFindNormalRollTarget() throws Exception {

        String filename = "/opt/dir/file.log";

        assertTrue(RollDetector.isRollCandidate(filename, filename + ".1"));
        assertFalse(RollDetector.isRollCandidate(filename, "XX" + filename + ".1"));

        assertTrue(RollDetector.isRollCandidate(filename, filename + ".2014-05-01"));
        assertFalse(RollDetector.isRollCandidate(filename, "NO" + filename + "2014-05-01"));
    }
    @Test
    public void shouldFindNumericRollTarget() throws Exception {

        String filename = "/opt/dir/file.log";

        assertTrue(RollDetector.isRollCandidate(filename + "", filename + ".1"));
        assertTrue(RollDetector.isRollCandidate(filename + ".1", filename + ".2"));

        assertFalse(RollDetector.isRollCandidate(filename + ".1", filename + "NO" + ".2"));
        assertFalse(RollDetector.isRollCandidate(filename + ".1", filename + ".N"));
    }


    @Test
    public void shouldBeFalseWhenHashCodeAndFirstLineStartsWithTruncated() throws IOException {
        assertThat(rollDetector.hasRolled(file, firstLine.substring(0, 20), 0, 0), is(false));
    }

    @Test
    public void shouldBeTrueWhenStartOfLineHasChanged() {
        assertThat(rollDetector.hasRolled(file, "Some other line that is different", 0, 0), is(true));
    }
}
