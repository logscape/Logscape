package com.liquidlabs.log.index;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.LogProperties;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LogFileTest {

    @Test
    public void shouldGrabHostnameCorrectly() throws Exception {
        String path  ="/home/logscape/LogscapeSAAS/logscape/work/LogServer_SERVER_/zvenyika.gomo@logscape.com/_SERVER_/10.28.0.80/_nxlog_/14Nov20/unknown-14Nov20.log";
        String[] hostnameFromPath = LogFile.getHostnameFromPath(path);
        assertEquals("10.28.0.80", hostnameFromPath[0]);
    }


    @Test
    public void shouldGrabHostnameFromWINPath_FWDFile() throws Exception {
        String unixPath = "D:\\opt\\" + LogProperties.getServiceDIRName() + "\\192.168.10.34\\d\\work\\log\\system.log";
        String[] server = new LogFile().getHostnameFromPath(unixPath);
        assertEquals("192.168.10.34", server[0]);
        assertEquals("\\d\\work\\log\\system.log", server[1]);
    }

    @Test
    public void shouldHandleServerEndAndNotBlowUp() throws Exception {
        String serverPath = "/volumes/lsdata/logscape-OnPrem/work/Logserver_SERVER_";
        String[] server = new LogFile().getHostnameFromPath(serverPath);
        // should not blowup and return null
        assertNull(server);
    }

    @Test
    public void shouldGrabHostnameFromPath_FWDFile() throws Exception {
        String unixPath = "/opt/" + LogProperties.getServiceDIRName() + "/192.168.10.34/var/log/system.log";
        String[] server = new LogFile().getHostnameFromPath(unixPath);
        assertEquals("192.168.10.34", server[0]);
        assertEquals("/var/log/system.log", server[1]);
    }
    @Test
    public void shouldDoToString() throws Exception {
        LogFile logFile = new LogFile();
        // check any null ptr exs
        logFile.toString();
    }


    @Test
    public void shouldBeWithInTime() throws Exception {
        LogFile logFile = new LogFile("name", 0, "fieldSetId","");
        logFile.update(100, new Line(0, 1, DateTimeUtils.currentTimeMillis() - DateUtil.HOUR, 100));
        logFile.update(100, new Line(0, 1, DateTimeUtils.currentTimeMillis(), 100));

        assertTrue(logFile.isWithinTime(new DateTime().minusHours(24).getMillis(), new DateTime().getMillis()));
    }

    @Test
    public void shouldAutoTag() throws Exception {
        LogFile logFile = new LogFile("/a/b/c/_AAA_/_bbb_/name", 0, "fieldSetId","cc");

        assertEquals("AAA,bbb,cc",logFile.getTags());
    }


}
