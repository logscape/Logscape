package com.liquidlabs.common;

import com.liquidlabs.common.collection.Arrays;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class StringUtilTest {




    @Test
    public void shouldSplitJsonContent() throws Exception {
        String splitJson = "_line!82!ts!2016-02-11 14:17:46.104 -0700!opp!CO!ti!2668b9b1-c38b-47d1-9eda-aeda1e6cbacf!n!CDN!v!3000!wu!LB!diu!IN!rfid!5110376381!t!COMMODITYID!fln!des!code!1029!dsc!The carrier cannot ship the item to the specified city!mrc!EBAY_0001!rid!5010435041!rt!BUSINESS!sid!STANDARD!ct!San Miguel Allende!reg!VA!cty!MX!hid!US_ELOVATIONS_KY!cid!5097617361!csid!100!hs!851180!hss!PB_CATEGORY!sd!08-10 KAWASAKI ZX10R ZX10 OEM MAIN ENGINE WIRING HARNESS MOTOR WIRE LOOM!url!http://www.ebay.com/itm/252088367911!szs!SELLER_PROVIDED!al_0_0_0_n!CDN!al_0_0_0_v!3000!atc!72!attc!1!cc!1!ccl_0_0_0_t!SALEPRICE!ccl_0_0_0_v!74.08!ccl_0_0_1_t!COPSHIPPING!ccl_0_0_1_v!0.0!cct!1!csil_0_0!100!ctgc!1!ctl_0_0_0!177932!dcl_0!MX!drl_0!VA!du!3!dv!1190!ebayid_0_0!252088367911!el_0_err_code!1029!el_0_err_dsc!The carrier cannot ship the item to the specified city!el_0_fl_0_fln!des!el_0_fl_1_fln!ct!emvi_0_0!7!flg!0!hsl_0_0!851180!hssi_0_0!PB_CATEGORY!idc!2!idl_0_0_0_rfid!5110376381!idl_0_0_0_t!COMMODITYID!idl_0_0_1_rfid!252088367911!idl_0_0_1_t!EBAYITEMID!ipl_cb!true!ipl_ccl_0_t!COPSHIPPING!ipl_ccl_0_v!0!ipl_cid!5097617361!ipl_cl_0_al_0_n!CDN!ipl_cl_0_al_0_v!3000!ipl_cl_0_ccl_0_t!SALEPRICE!ipl_cl_0_ccl_0_v!74.08!ipl_cl_0_ccl_1_t!COPSHIPPING!ipl_cl_0_ccl_1_v!0.0!ipl_cl_0_csid!100!ipl_cl_0_ctl_0!177932!ipl_cl_0_emv!7!ipl_cl_0_hs!851180!ipl_cl_0_hss!PB_CATEGORY!ipl_cl_0_idl_0_rfid!5110376381!ipl_cl_0_idl_0_t!COMMODITYID!ipl_cl_0_idl_1_rfid!252088367911!ipl_cl_0_idl_1_t!EBAYITEMID!ipl_cl_0_qty!1!ipl_cl_0_sd!08-10 KAWASAKI ZX10R ZX10 OEM MAIN ENGINE WIRING HARNESS MOTOR WIRE LOOM!ipl_cl_0_sz_diu!IN!ipl_cl_0_sz_h!7.0!ipl_cl_0_sz_l!10.0!ipl_cl_0_sz_w!5.0!ipl_cl_0_sz_wgt!5.0!ipl_cl_0_sz_wu!LB!ipl_cl_0_szs!SELLER_PROVIDED!ipl_cl_0_url!http://www.ebay.com/itm/252088367911!ipl_des_ct!San Miguel Allende!ipl_des_cty!MX!ipl_des_reg!VA!ipl_ds_mnd!2!ipl_ds_mxd!10!ipl_hid!US_ELOVATIONS_KY!ipl_mrc!EBAY_0001!ipl_ret_rid!5010435041!ipl_ret_rt!BUSINESS!ipl_reta_ct!Ormond Beach!ipl_reta_cty!US!ipl_reta_reg!FL!ipl_sid!STANDARD!mrc!EBAY_0001!opp!CO!qty_0_0!1!sat!true!spr_0_0!74.08!sz_0_0_diu!IN!sz_0_0_h!7.0!sz_0_0_l!10.0!sz_0_0_w!5.0!sz_0_0_wgt!5.0!sz_0_0_wu!LB!szsi_0_0!SELLER_PROVIDED!ti!2668b9b1-c38b-47d1-9eda-aeda1e6cbacf!ts!2016-02-11 14:17:46.104 -0700!wgt_0_0!5.0!";

        String[] result = StringUtil.splitFast2(splitJson, '!');
        String[] split = splitJson.split("!");
        assertEquals(split.length, result.length);
    }


    String splitData = "123line!1!device_id!woohoo!";
    String splitLong = "line!1!device_id!WEST_STREET!duration!20!policy_id!33!proto!6!zone!Trust!action!Permit!sent!234!rcvd!0!src!10.181.100.200!dst!10.0.6.211!src_port!16848!dst_port!9080!port!16848!session_id!6393!reason!Close!start_time!2015-01-09 23:59:54!_timestamp!1399781076!";


    @Test
    public void shouldSanitizeFileName() throws Exception{
        String before = "hello-thisis/afile/name.log";
        String after = "hellothisisafilename.log";
        String replaced = StringUtil.replaceAll(new String[]{"-", "/"}, "", before);
        assertTrue(after.equals(replaced));
    }
    @Test
    public void shouldSplitByteArray() throws Exception {

        String[] result = StringUtil.splitFast(splitLong, '!');

        System.err.println("Running....");

        int limit = 5 * 1000 ;//* 1000;


        runBenchmark("split1", limit, new Runnable() {
            public void run() {
                StringUtil.splitFast1(splitLong, '!');
            }
        });
        String[] strings = StringUtil.splitFast2(splitLong, '!');
        String[] correctSplit = splitLong.split("!");
        System.out.println("First:" + strings[0]+ "<:>" + correctSplit[0] );
        System.out.println("Last:" + strings[strings.length-1]+ "<:>" + correctSplit[correctSplit.length-1] );
        assertEquals(strings.length, correctSplit.length);
        runBenchmark("split2", limit, new Runnable() {
            public void run() {
                StringUtil.splitFast2(splitLong, '!');
            }
        });
        runBenchmark("split3", limit, new Runnable() {
            public void run() {
                StringUtil.splitFast3(splitLong, '!');
            }
        });

        runBenchmark("split4", limit, new Runnable() {
            public void run() {
                StringUtil.splitFast4(splitLong, '!');
            }
        });

//        runBenchmark("split5", limit, new Runnable() {
//            public void run() {
//                StringUtil.splitFast5(splitLong, '!');
//            }
//        });
//
//
//        assertEquals(4, result.length);
    }

    private void runBenchmark(String name, int limit, Runnable runnable) {
        for (int count = 0; count < 10; count++) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < limit; i++) {
                runnable.run();
            }
            long end = System.currentTimeMillis();
            System.out.println(name + " Rate per sec:" + limit /  ((end - start) / 1000.0));
        }
    }

    @Test
    public void shouldTruncate(){
        String src = "hello this is a test of the utmost calibre";
        assertTrue(StringUtil.truncateString(13, src).length() == 27);
        assertTrue(StringUtil.truncateString(500, src).equals(src));
    }
    @Test
    public void shouldWrapString() throws Exception {
        String result = StringUtil.wrapCharArray("Test".toCharArray());
        assertEquals("Test", result);
    }


    @Test
    public void shouldRoundProperly() throws Exception {

        double vvv = StringUtil.roundDouble(92000.123456789);
        assertTrue("Got:" + vvv, vvv > 92000);
    }

    @Test
    public void shouldParseInt() throws Exception {
        long time = System.currentTimeMillis();
        String timeString = time + "";
        assertNull(StringUtil.isInteger(timeString));
        assertNotNull(StringUtil.isInteger(Integer.MAX_VALUE +""));
        assertNotNull(StringUtil.isInteger(Integer.MIN_VALUE +""));
        assertNotNull(StringUtil.isInteger("0"));

        assertEquals(time, StringUtil.longValueOf(timeString));
    }



    @Test
    public void shouldEscapeXMLEntities() throws Exception {
        String testXML = "<xml>\n"+
                "<item><Email>Ulrich.demaeyer@alpha-card.com</Email>\t\t<IP-By>78.23.235.181</IP-By>\t\t<IP-By_view>null</IP-By_view><_pages_>1</_pages_>\t\t<CP-By>Alpha-card</CP-By>\t\t<CP-By_view>null</CP-By_view></item>\n"+
                "<item><Email>rs.online.0@gmail.com</Email>\t\t<IP-By>206.81.136.61</IP-By>\t\t<IP-By_view>null</IP-By_view>\t\t<CP-By>RSC</CP-By>\t\t<CP-By_view>null</CP-By_view></item>\n"+
                "<item><Email>robert.diaz@ogilvy.com</Email>\t\t<IP-By>199.20.45.21</IP-By>\t\t<IP-By_view>null</IP-By_view>\t\t<CP-By>Ogilvy & Mather</CP-By>\t\t<CP-By_view>null</CP-By_view></item>\n"+
                "<item><Email>test@gmail.com</Email>\t\t<IP-By>125.16.138.147</IP-By>\t\t<IP-By_view>null</IP-By_view>\t\t<CP-By>TestLine</CP-By>\t\t<CP-By_view>null</CP-By_view></item>\n"+
                "<item><Email>Guruindya@gmail.com</Email>\t\t<IP-By>125.16.138.142</IP-By>\t\t<IP-By_view>null</IP-By_view>\t\t<CP-By>Testers</CP-By>\t\t<CP-By_view>null</CP-By_view></item>\n"+
                "<item><Email>Guruindya@hotmail.com</Email>\t\t<IP-By>125.16.138.142</IP-By>\t\t<IP-By_view>null</IP-By_view>\t\t<CP-By>Testers</CP-By>\t\t<CP-By_view>null</CP-By_view></item>\n"+
                "</xml>";

        // i.e. <crap>stuff & some </crap>
        // testing: Ogilvy & Mather => Ogilvy &amp; Mather
        String s = StringUtil.escapeXMLEntities(testXML);
        System.out.println(s);
        assertTrue(s.contains("Ogilvy &amp; Mather"));


    }
    @Test
    public void shouldDoDouble() throws Exception {
        String doubleToString = StringUtil.doubleToString(1234.45);
        assertEquals("1234.45", doubleToString);

        long start = System.currentTimeMillis();

//		while (true) {
        int limit = 100;// 10000 * 1000;
        for (int i = 0; i < limit; i++) {
            Double double1 = new Double(1234.45);
//				double1.toString();
            StringUtil.doubleToString(double1);
//				StringBuilder s = new StringBuilder();
//				DoubleToString.append(s, 1234.45);
//				System.out.println("---" + s);
        }
        long end = System.currentTimeMillis();
        System.out.println("Elapsed:" + (end - start));
//		}

    }

    @Test
    public void shouldBeDbouleSpaced() throws Exception {
        String stuff = "2011-03-29 14:09:02,359 INFO agent-sched-12-1 (StatsLogger)	AGENT envy14-11050 CPU:18 MemFree:1041 MemUsePC:73.26 DiskFree:358576 DiskUsePC:22.23 SwapFree:3537 SwapUsePC:13.63\r\n" +
                "\r\n" +
                "2011-01-17 15:02:02,732 INFO agent-sched-12-1 (StatsLogger)	AGENT envy14-11050-0 MEM MB MAX:1017 COMMITED:271 USED:36 AVAIL:981 SysMemFree:1097 TimeDelta:0\r\n" +
                "\r\n" +
                "2011-01-17 15:03:02,715 INFO agent-sched-12-1 (StatsLogger)	AGENT envy14-11050 CPU:7 DiskFree:385774 SwapFree:4095\r\n" +
                "\r\n" +
                "2011-01-17 15:03:02,716 INFO agent-sched-12-1 (StatsLogger)	AGENT envy14-11050-0 MEM MB MAX:1017 COMMITED:271 USED:25 AVAIL:991 SysMemFree:1075 TimeDelta:0\r\n" +
                "\r\n" +
                "2011-01-17 15:04:02,735 INFO agent-sched-12-1 (StatsLogger)	AGENT envy14-11050 CPU:7 DiskFree:385774 SwapFree:4095\r\n" +
                "\r\n" +
                "\r\n" +
                "\r\n" +
                "";
        assertTrue(StringUtil.isDoubleSpaced(stuff));
    }
    @Test
    public void shouldWorkWithDoubleSpace() throws Exception {
        assertFalse(StringUtil.isDoubleSpaced("one\ntwo\nthree"));
        assertFalse(StringUtil.isDoubleSpaced("one\n\ntwo\nthree"));
        assertTrue(StringUtil.isDoubleSpaced("one\n\ntwo\n\nthree"));
    }

    @Test
    public void shouldRunDoublePerformanceBenchmark() throws Exception {
        long start = System.currentTimeMillis();
        String number = "10000.0";
        String string = "1000aa";
        int limit = 1000 * 1000;
        for (int i = 0; i < limit; i++) {
            StringUtil.isDouble(number);
            StringUtil.isDouble(string);
        }

        long end = System.currentTimeMillis();
        System.out.println("Elapsed:" + (end - start));
    }

    @Test
    public void shouldTestIntValue() throws Exception {
        assertTrue(StringUtil.isIntegerFast("100"));
        assertFalse(StringUtil.isIntegerFast("100a"));
        assertFalse(StringUtil.isIntegerFast("100.0"));

    }
    @Test
    public void shouldRunIntegerPerformanceBenchmark() throws Exception {
        long start = System.currentTimeMillis();
        String number = "10000";
        String string = "1000aa";
        int limit = 1000 * 1000;
        boolean old = false;
        for (int i = 0; i < limit; i++) {
            // 3322
            if (old) {
                StringUtil.isInteger(number);
                StringUtil.isInteger(string);
            } else {
                StringUtil.isIntegerFast(number);
                StringUtil.isIntegerFast(string);
            }
        }

        long end = System.currentTimeMillis();
        System.out.println("Elapsed:" + (end - start));
    }


    @Test
    public void shouldExtractLastSubstring() throws Exception {
        String filePath = "/var/log/stuff.log";
        String result = StringUtil.lastSubstring(filePath, "/", "/");
        assertEquals("log", result);
    }
    @Test
    public void shouldSplitFastWithExcess() throws Exception {

        String line =  "one two three four";
        String[] split = StringUtil.splitFast(line, " ".charAt(0), 3, false);
        assertEquals("one", split[0]);
        assertEquals("two", split[1]);
        assertEquals("three four", split[2]);

    }

    @Test
    public void shouldSubstringAnItem() throws Exception {

        String line =  "one two three";
        assertEquals("one", StringUtil.splitFastSCAN(line, " ".charAt(0), 1));
        assertEquals("two", StringUtil.splitFastSCAN(line, " ".charAt(0), 2));
        assertEquals("three", StringUtil.splitFastSCAN(line, " ".charAt(0), 3));
        assertEquals("", StringUtil.splitFastSCAN(line, " ".charAt(0), 4));

    }
    @Test
    public void shouldSplitFast() throws Exception {

        String line =  "one two three";
        String[] split = StringUtil.splitFast(line, " ".charAt(0), 3, false);
        assertEquals("one", split[0]);
        assertEquals("two", split[1]);
        assertEquals("three", split[2]);

        String line2 =  "one  three";
        String[] split2 = StringUtil.splitFast(line2, " ".charAt(0), 3, false);
        assertEquals("one", split2[0]);
        assertEquals("", split2[1]);
        assertEquals("three", split2[2]);


        String line3 =  "one two three";
        String[] split3 = StringUtil.splitFast(line3, " ".charAt(0), 3, true);
        assertEquals("one", split3[1]);
        assertEquals("two", split3[2]);
        assertEquals("three", split3[3]);

    }
    @Test
    public void shouldHandleMCharSPLIT() throws Exception {
        String delim = "or";
        String stringToSplit = delim + "1" + delim + delim + "3";
        String[] split = StringUtil.split("or", stringToSplit);
        String[] splitRE = stringToSplit.split("or");
        assertEquals(Arrays.toString(splitRE), Arrays.toString(split));
        assertEquals(splitRE.length, split.length);
    }

    @Test
    public void shouldHandleEmptyBits() throws Exception {
        String stringToSplit = ",1,,3";
        String[] split = StringUtil.split(",", stringToSplit);
        String[] splitRE = stringToSplit.split(",");
        assertEquals(Arrays.toString(splitRE), Arrays.toString(split));
        assertEquals(splitRE.length, split.length);
    }


    @Test
    public void testWhiteSpaceIsNotInTheWay() throws Exception {
        String[] split = StringUtil.split("|", "a|b or c");
        assertEquals(2, split.length);
    }
    @Test
    public void testNoTokenExists() throws Exception {
        String[] split = StringUtil.split("|", "a");
        assertEquals("Got:" + java.util.Arrays.toString(split), 1, split.length);
    }

    @Test
    public void testStringSplitWith2Chars() throws Exception {
        String[] split = StringUtil.split("|", "a|b");
        assertEquals(2, split.length);
    }
    @Test
    public void testStringSplitting() throws Exception {
        String[] split = StringUtil.split("|", "a|b");
        assertEquals(2, split.length);
    }

    @Test
    public void shouldStringContainsObviousStuff() throws Exception {
        String line = "stuff Load() stuff stuff";
        assertTrue(StringUtil.containsIgnoreCase(line, "load()"));
    }

    @Test
    public void shouldContainsIgnoreCase() {
        assertTrue(StringUtil.containsIgnoreCase("abCdeFgH", "BCD"));
    }

    @Test
    public void shouldNotContainsIgnoreCase() {
        assertFalse(StringUtil.containsIgnoreCase("abCdeFgH", "XyZ"));
    }

    @Test
    public void shouldNotContainsIgnoreCase2() {
        assertFalse(StringUtil.containsIgnoreCase("ABC", "abcde"));
    }

    @Test
    public void shouldMatch() {
        assertTrue(StringUtil.containsIgnoreCase("A", "a"));
    }

    @Test
    public void shouldFoo() {
        final String string = "abcdefghijklmnopqrstuvwxyz";
        doIt(string);
        doIt(string);
        doIt(string);
        doIt(string);
        doIt(string);
        doIt(string);
        doIt(string);
    }

    private void doIt(String string) {
        long v1 = doContainsIgnoreCase(string, "VWXY");
        long v2 = doContainsIgnoreCase2(string, "VWXY");
        long v3 = doContainsIgnoreCase3(string, "VWXY");
        System.out.println("LookupTable: " + v1 + " Arith: " + v2 + " toLower: " + v3);
    }

    private long doContainsNoIgnore(String string, String vwxyz) {
        System.gc();
        boolean b =false;
        long start = System.currentTimeMillis();
        for(int i = 0; i<100000; i++) {
            b  = string.contains(vwxyz);
        }
        long end = System.currentTimeMillis();
        if(b) return end - start;
        return -1;
    }

    @Test
    public void shouldContainsEdgeCases() throws Exception {

        boolean containsIgnoreCase = StringUtil.containsIgnoreCase("aa", "");
        assertEquals("aa".contains(""), containsIgnoreCase);

        StringUtil.containsIgnoreCase("", null);

        containsIgnoreCase = StringUtil.containsIgnoreCase("", "aa");
        assertEquals("".contains("aa"), containsIgnoreCase);
    }

    private long doContainsIgnoreCase(String string, String vwxy) {
        System.gc();
        boolean b = false;
        long start = System.currentTimeMillis();
        for(int i = 0; i<100000; i++) {
            b = StringUtil.containsIgnoreCase(string, vwxy);
        }
        long end = System.currentTimeMillis();
        if(b) return end - start;
        return -1;
    }

    private long doContainsIgnoreCase2(String string, String vwxy) {
        System.gc();
        boolean b = false;
        long start = System.currentTimeMillis();
        for(int i = 0; i<100000; i++) {
            b = StringUtil.containsIgnoreCase2(string, vwxy);
        }
        long end = System.currentTimeMillis();
        if(b) return end - start;
        return -1;
    }

    private long doContainsIgnoreCase3(String string, String vwxy) {
        System.gc();
        boolean b = false;
        long start = System.currentTimeMillis();
        for(int i = 0; i<100000; i++) {
            b = StringUtil.containsIgnoreCase3(string, vwxy);
        }
        long end = System.currentTimeMillis();
        if(b) return end - start;
        return -1;
    }

    private long doStringIgnoreCase(String string, String vwxy) {
        System.gc();
        int k = -1;
        long start = System.currentTimeMillis();
        for(int i = 0; i<100000; i++) {
            k = string.toLowerCase().indexOf(vwxy.toLowerCase());
        }
        long end = System.currentTimeMillis();
        if(k>-1)
            return end - start;
        return k;
    }


}
