package com.liquidlabs.common.regex;

import static org.junit.Assert.*;

import org.junit.Test;

public class RegExpUtilTest {

    @Test
    public void shouldWorkWithCommaStrings() throws Exception {

        String[] fileRegexps =  RegExpUtil.getCommaStringAsRegExp("*.log, *.csv, !xxx", true);
        for (String fileRegexp : fileRegexps) {
            assertFalse(fileRegexp.contains(" "));
        }
    }



    @Test
    public void shouldWorkWithExcludeOnMultiPatterns() throws Exception {
        boolean match = RegExpUtil.isMatch("good", new String[] {  "good","!ood" });
        assertFalse(match);

        boolean match2 = RegExpUtil.isMatch("good", new String[] {  "good","!bad" });
        assertTrue(match2);
    }

    @Test
    public void shouldDetectMLineOnNonMLine() throws Exception {
        assertFalse(RegExpUtil.isMultiline("one two three"));
        String line = "Jun 17 15:00:48 172.16.2.2 Jun 17 2011 15:37:09: %ASA-6-302013: Built inbound TCP connection 53384275 for INSIDE:10.1.1.211/62411 (10.1.1.211/62411) to identity:10.1.5.3/443 (10.1.5.3/443)\r\n";
        assertFalse(RegExpUtil.isMultiline(line));
    }
    @Test
    public void shouldDetectMLineOnMLine() throws Exception {
        assertFalse(RegExpUtil.isMultiline("one two three"));
        String line = "Jun 17 15:00:48 172.16.2.2 Jun 17 2011 15:37:09: %ASA-6-302013:\n Built inbound TCP connection 53384275 for INSIDE:10.1.1.211/62411 (10.1.1.211/62411) to identity:10.1.5.3/443 (10.1.5.3/443)\r\n";
        assertTrue(RegExpUtil.isMultiline(line));
    }

    @Test
    public void shouldValueValueOf() throws Exception {
        String nextWordAfter = RegExpUtil.getNextWordAfter("token:", "this token:stuff all that");
        assertEquals("stuff", nextWordAfter);
    }


    @Test
    public void shouldValueOff2() throws Exception {
        String token = "vso.resource.type\" value=\"";
        String text =
                "            <add key=\"vso.resource.type\" value=\"Management\"/>\n" +
                        "        <!-- Boot the manager in HA mode or not. HA uses more resources -->\r\n" +
                        "        <add key=\"vso.ha.enabled\" value=\"false\"/>\r\n";

        String word = RegExpUtil.getNextWordAfter(token, text);
        assertEquals("Management", word);
    }
    @Test
    public void shouldReadValueOfHeap() throws Exception {
        String token = "-Xmx";
        String text = "vmargs=-Xms256M -Xmx1024M -XX:MaxHeapFreeRatio=20 -XX:MinHeapFreeRatio=10 -XX:MaxPermSize=256m -XX:+UseConcMarkSweepGC -XX:+HeapDumpOnOutOfMemoryError -Xrs \r\n" +
                "\r\n" +
                "sysprops=-DLOGSCAPE -Dvso.boot.lookup.replication.port=15000 -Dvso.lookup.peers=stcp://localhost:15000,stcp://localhost:25000 -Dvso.resource.type=Management -Dlog4j.configuration=./agent-log4j.properties -Dweb.app.port=8080 -Dfile.encoding=ISO-8859-1 -Dvso.group=LDN \r\n" +
                "";
        String word = RegExpUtil.getNextWordAfter(token, text);
        assertEquals("1024M", word);
    }

}
