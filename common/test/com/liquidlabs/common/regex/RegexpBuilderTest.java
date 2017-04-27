package com.liquidlabs.common.regex;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.StringContains.containsString;


public class RegexpBuilderTest {

    private RegexpBuilderA regexpBuilder;

    @Before
    public void setup() {
        regexpBuilder = new RegexpBuilderA();
    }

//    @Test
    public void shouldGiveGoodGroups() throws Exception {
        String line = "2010/07/09 23:59:59|datapair|info|119de7b35985488|bzo|43935fe0-c66c-412c-9396-d43f0913ea40|rep||G8K,U2A,I8T,M4F,A1N,KGES6N";
//		String expression ="^(*) (*)|(*)|(*)|(*)|(*)|(*)|(*)|(*)";
        String expression = "info|(w)|(w)";
        String output = "";
        for (int i = 0; i < 10; i++) {
            output = RegExpUtil.testJRegExp(expression, line);
        }
        System.out.println(output);

    }

//    @Test
    public void giveGoodCOllectivePerformanceStatsOnString() throws Exception {
        String line = "2010-06-25 21:10:13 -0500: rtb|CAESEKgAwa-9zztXY60xCPI3fuA_1||snippet|<script src=\"http://a.collective-media.net/cmadj/cm.dcrtb/X_224578884;sz=160x600;site=http%3A%2F%2Fwww.playlist.com%2Fsearchbeta%2Ftracks;contx=none;btg=;click0=http://www.americansforthearts.org;ord=1276723506?\">";
        String regexp = "^(*) (*) (*) (*)|(*)|(*)|(*)";

        for (int i = 0; i < 5; i++) {
            System.out.println("============================");
            String output = RegExpUtil.testJRegExp(regexp, line);
            System.out.println("1out:" + output);
            String output2 = RegExpUtil.testJavaRegExp(regexp, line);
            System.out.println("2out:" + output2);
        }
    }

    @Test
    public void shouldDoGrouping() throws Exception {
        String line = "one Apple two";
        String regexp = ".* (Apple|Orange) .*";

        String output = RegExpUtil.testJRegExp(regexp, line);
        assertThat(output, Matchers.containsString("<b>Group count</b>:2"));
    }

    @Test
    public void shouldSplitSingleLine() throws Exception {
        String line = "99.166.64.35 - - [05/Mar/2010:07:50:51 -0500] \"GET /plugins/system/rokbox/themes/light/rokbox-config.js HTTP/1.1\" 200 2598 \"http://www.liquidlabs-cloud.com/products/logscape.html\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_2; en-us) AppleWebKit/531.21.8 (KHTML, like Gecko) Version/4.0.4 Safari/531.21.10\"";
        String regexp = "(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\.(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s(.*)";

        String output = RegExpUtil.testJRegExp(regexp, line);
        assertThat(output, Matchers.containsString("<b>Group count</b>:14"));
    }

    @Test
    public void shouldSplitRegExpMultiline() throws Exception {
        String line = "one\r\ntwo\r\nthree";
        String regexp = "(.*)\\s(two)\\s(.*)";

        String output = RegExpUtil.testJavaRegExp(regexp, line);
        assertFalse(output, output.contains("No match"));
    }

    @Test
    public void shouldSplitRegExpMultilineException() throws Exception {
        String line = "java.lang.RuntimeException: java.lang.IllegalArgumentException: Hash of public key has changed - suspect tampering\r\n" +
                "	at com.liquidlabs.common.license.LicenseValidator.&lt;init>(LicenseValidator.java:32)\r\n" +
                "	at com.liquidlabs.common.license.LicenseManager.&lt;init>(LicenseManager.java:18)\r\n" +
                "	at com.liquidlabs.admin.AdminSpaceImpl.setupPreregisteredData(AdminSpaceImpl.java:125)\r\n" +
                "	at com.liquidlabs.admin.AdminSpaceImpl.start(AdminSpaceImpl.java:44)\r\n" +
                "	at com.liquidlabs.admin.AdminSpaceImpl.boot(AdminSpaceImpl.java:167)\r\n" +
                "	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)";
//		String regexp = "(.*)\\n(two)\\n(.*)";
        String regexp = "(.*)";

//		String output = RegExpUtil.testJRegExp(regexp, line);
        String output = RegExpUtil.testJavaRegExp(regexp, line);
        assertFalse(output, output.contains("No match"));
    }

    @Test
    public void shouldSplitRegExp() throws Exception {
        String line = "2009-12-08 12:16:02,800 INFO agent-sched-11-1 (agent.ResourceAgent)	AGENT LON-XPNAVERY3-12000 CPU:0 DiskFree:7332 SwapFree:4095 id=100";
        String expression = regexpBuilder.getSplitRegExp(line, "AGENT");
        MatchResult matches = RegExpUtil.matches(expression, line);
        assertTrue("Regexp didnt match, expr:" + expression, matches.match);

    }

    @Test
    public void testShouldHandleEmbeddedDelimsProperly() throws Exception {
        String line = "2009-12-08 12:16:02,800 INFO agent-sched-11-1 (agent.ResourceAgent)	AGENT LON-XPNAVERY3-12000 CPU:0 DiskFree:7332 SwapFree:4095 id=100";
        String tagString = "RE_Date1 RE_Time4 RE_UCaseWord agent-sched-11-1 \\(RE_PackageWord\\) RE_UCaseWord LON-XPNAVERY3-12000 CPU:RE_Integer DiskFree:RE_Integer SwapFree:RE_Integer id=RE_Integer";

//		String tagString = regexpBuilder.getTagString(line);
        String extractToRegexp = regexpBuilder.extractToRegexp(tagString);
        boolean match = regexpBuilder.isMatch(extractToRegexp, line);
        assertTrue("Regexp didnt match", match);
//		assertTrue("should not have had an integer", extractToRegexp.indexOf("Integer") == -1);


    }


    @Test
    public void testShouldDoListOfStrings() throws Exception {
        List<String> stuff = Arrays.asList("RE_Word | verbose(true)", "RE_Word | verbose(false)");
        List<String> result = regexpBuilder.extractToRegexp(stuff);
        assertTrue(result.toString(), result.toString().contains("\\w+"));
        assertTrue(result.toString().contains(" | verbose(true)"));
        assertTrue(result.toString().contains(" | verbose(false)"));
    }

    @Test
    public void testShouldResolveTagsAndExecuteRegexp() throws Exception {
        String string = "2009-12-07 13:48:54,452 INFO ResourceAgent-2-1 (agent.ResourceAgent)	Starting NonForked:LON-XPNAVERY3";//-12000-0:boot-1.0:LookupSpace";
        String tagString = regexpBuilder.getTagString(string);

        String regExp = regexpBuilder.extractToRegexp(tagString);
        String noGroupedString = regexpBuilder.removeGroups(string);
        String testJRegExp = RegExpUtil.testJRegExp(regExp, noGroupedString);
        assertTrue(testJRegExp.contains("Performance"));
    }

    @Test
    public void testShouldResolveCSV() throws Exception {
        String tagString = regexpBuilder.getTagString("1242973152454,4042323260,demo,12,57,5015,1,3494388101,80,media.imeem.com,/pl/R3s6l1BZxw/aus=false/,2030491983,1830,0,1242973150,450,2,686,2558,4,39,12,6,50402048,0,0,0,2030491983,Mozilla,http://sweetlife-of-kelly.blogspot.com/,");
        System.out.println("TagString:" + tagString);
        assertThat(tagString.length(), is(greaterThan(0)));
        assertThat(tagString, not(containsString("media.imeem.com")));
    }

    @Test
    public void testGiveMeTAGStringWithoutLHSResolved() throws Exception {
        String tagString = regexpBuilder.getTagString("something:value");
        System.out.println(tagString);
        assertTrue(tagString.contains("something:"));
        assertTrue(!tagString.contains("value"));
    }

    @Test
    public void testGiveMeTAGStringWithoutLHSResolvedWithEQ() throws Exception {
        String tagString = regexpBuilder.getTagString("something=value");
        assertTrue(tagString.contains("something="));
        assertTrue(!tagString.contains("value"));
    }

    @Test
    public void testGiveMeTAGString() throws Exception {
        String tagString = regexpBuilder.getTagString("2009-12-07 13:48:54,452 INFO ResourceAgent-2-1 (agent.ResourceAgent)	Starting NonForked:LON-XPNAVERY3-12000-0:boot-1.0:LookupSpace");
        assertTrue(tagString.length() > 0);
    }

//    @Test
    public void should() throws Exception {
        // ^(("(?:[^"]|"")*"|[^,]*)(,("(?:[^"]|"")*"|[^,]*))*)$
        String csv = "89898998,foo,\",90909\",foo,90909,kbkh,90808,0-98098";
        MatchResult matchResult = RegExpUtil.matches("^([^,]*)(,([^,])*)*$", csv);
        MatchResult matchResult2 = RegExpUtil.matches("^((\"(?:[^\"]|\"\")*\"|[^,]+)(,(\"(?:[^\"]|\"\")*\"|[^,]+))+)$", csv);
        String[] groups = matchResult.getGroups();
        for (int i = 0; i < groups.length; i++) {
            System.out.println("Group: " + i + " " + groups[i]);
        }
        System.out.println("Size: " + groups.length);
        System.out.println(Arrays.toString(groups));

    }

    @Test
    public void testHttpAddress() throws Exception {
        String tagString = regexpBuilder.getTagString("http://sweetlife-of-kelly.blogspot.com/is/here");
        assertEquals("RE_HttpAddress", tagString);

    }

    @Test
    public void testShouldGetPackageWord() throws Exception {
        String tagString = regexpBuilder.getTagString("agent.ResourceAgent");
        assertEquals("RE_PackageWord", tagString);
    }

    @Test
    public void testShouldGetTimeFormat5() throws Exception {
        String tagString = regexpBuilder.getTagString("13:48:54.100");
        assertEquals("RE_Time5", tagString);
    }

    @Test
    public void testShouldGetTimeFormat3() throws Exception {
        String tagString = regexpBuilder.getTagString("13:48:54,100");
        assertEquals("RE_Time4", tagString);
    }

    @Test
    public void testShouldGetTimeFormat2() throws Exception {
        String tagString = regexpBuilder.getTagString("13:48:54");
        assertEquals("RE_Time2", tagString);
    }

    @Test
    public void testShouldGetDateFormat3() throws Exception {
        String tagString = regexpBuilder.getTagString("12/22/09");
        assertEquals("RE_Date3", tagString);
    }

    @Test
    public void testShouldGetDateFormat2() throws Exception {
        String tagString = regexpBuilder.getTagString("2009-Feb-07");
        assertEquals("RE_Date2", tagString);
    }

    @Test
    public void testShouldGetDateFormat1() throws Exception {
        String tagString = regexpBuilder.getTagString("2009-12-07");
        assertEquals("RE_Date1", tagString);
    }

    @Test
    public void testShouldGetIPAddress() throws Exception {
        String tagString = regexpBuilder.getTagString("192.168.1.100");
        assertEquals("RE_IpAddress", tagString);
    }

    @Test
    public void testShouldGetEmail() throws Exception {
        String tagString = regexpBuilder.getTagString("me@myStuff.com");
        assertEquals("RE_Email", tagString);
    }

    @Test
    public void testShouldGetDecimal() throws Exception {
        String tagString = regexpBuilder.getTagString("12345.50");
        assertEquals("RE_Decimal", tagString);
    }

    @Test
    public void testShouldGetInteger() throws Exception {
        String tagString = regexpBuilder.getTagString("12345");
        assertEquals("RE_Integer", tagString);
    }

    @Test
    public void testShouldMatchMe() throws Exception {
//		boolean isMatch =  regexpBuilder.isMatch("([0-9]+\\:[0-9]+\\.[0-9]+\\,[0-9]+)","12:34.06,100");
        boolean isMatch = regexpBuilder.isMatch("([0-9]+\\-[0-9]+\\-[0-9]+)", "2009-12-07");
        assertTrue(isMatch);
        boolean isNotDigit = regexpBuilder.isMatch("\\d+", "12a345");
        assertTrue(!isNotDigit);
    }
}
