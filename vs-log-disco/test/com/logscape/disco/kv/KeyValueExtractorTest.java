package com.logscape.disco.kv;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.logscape.disco.indexer.Pair;
import com.logscape.disco.kv.KeyValueExtractor;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 28/03/2013
 * Time: 10:31
 * To change this template use File | Settings | File Templates.
 */
public class KeyValueExtractorTest {

    /**
     * Tree supports
     * json style
     * "key": "val"
     * "key": 999
     * "key": val,
     *
     * KV style
     *  service=?URLs?
     *
     *  CPU:100
     *
     *
     */

    // NOTE Q - is swapped out for " - but because its embedded within json its a prick to read properly
    // Optional Match = alpha, alhpaNumeric or none

//    { "dfp": [
//            {
//                "name": "json",
//                "token": "Q",
//                "children": [
//                        {
//                                "token": "Q: Q", "match": "alphaNumeric",  "children": [ {"token": "Q" } ]
//                        },
//                        {
//                            "token": "Q: ",  "match": "alphaNumeric",      "children": [ {"token": "," },
//                                                                                    {"token": " " }]
//                        }]
//            },{
//                "name": "kv",
//                    "token": " ",
//                    "children": [
//                        {
//                            "token": "=Q",  "match": "alphaNumeric", "children": [ {"token": "Q" } ]
//                        },
//                        {
//                            "token": ":",   "match": "alphaNumeric", "children": [ {"token": " ","match": "alphaNumeric" } ]
//                        }
//                    ]
//            }
//            ] }



    String parseTree =
            " { \"dfp\": [\n" +
                    "            {\n" +
                    "                \"name\": \"json\",\n" +
                    "                \"token\": \"Q\",\n" +
                    "                \"children\": [\n" +
                    "                        {\n" +
                    "                                \"token\": \"Q: Q\", \"match\": \"alphaNumeric\",  \"children\": [ {\"token\": \"Q\" } ]\n" +
                    "                        },\n" +
                    "                        {\n" +
                    "                            \"token\": \"Q: \",  \"match\": \"alphaNumeric\",      \"children\": [ {\"token\": \",\" },\n" +
                    "                                                                                    {\"token\": \" \" }]\n" +
                    "                        }]\n" +
                    "            }" +
                    "," +
                    "{\n" +
                    "                \"name\": \"kv\",\n" +
                    "                    \"token\": \" \",\n" +
                    "                    \"children\": [\n" +
                    "                        {\n" +
                    "                            \"token\": \"=Q\",  \"match\": \"alphaNumeric\", \"children\": [ {\"token\": \"Q\" } ]\n" +
                    "                        },\n" +
                    "                        {\n" +
                    "                            \"token\": \":\",   \"match\": \"alphaNumeric\", \"children\": [ {\"token\": \" \",\"match\": \"alphaNumeric\" } ]\n" +
                    "                        }\n" +
                    "                    ]\n" +
                    "            }\n" +
                    "            ] }";
    private KeyValueExtractor kve;

    @Before
    public void before() {
        kve = new KeyValueExtractor(parseTree);
    }

    @Test
    public void testShouldExtractIt() throws Exception {
        //String line = " 2013-03-30 19:35:29,703 INFO dashboard-log-stats-17-1 (server.DashboardServiceDelegator)\t DashboardService Chronos MEM MB MAX:494 COMMITED:25 USED:16 AVAIL:478 ";
        String line = " COMMITED:25 MAX:494 USED:16 AVAIL:478 ";
        List<Pair> fields = kve.getFields(line);
        System.out.println("GOT:" + fields);
        assertEquals(4, fields.size());
    }
//    @Test DodgyTest? FileName - maybe shouldn't be in a test?
    public void testShouldSpeed() throws Exception {

//        while (true) {
        String file = "/Volumes/Media/LOGSCAPE/TRUNK/LogScape/master/build.run/logscape/work/agent.log";
//        String file = "D:\\work\\LOGSCAPE\\TRUNK\\master\\build.run\\logscape\\work\\agent.log";

    //    while (true) {
            RAF raf = RafFactory.getRafSingleLine(file);
            String line = "";
            long start = System.currentTimeMillis();
            int lineCount = 0;
            int count = 0;

            while ((line = raf.readLine()) != null) {
//                System.out.println(line);
                List<Pair> fields = kve.getFields(line);
                if (fields.size() > 0) {
                    count ++;
//                    System.out.println("Fields:" + fields);
                }
                lineCount++;
            }
            long end = System.currentTimeMillis();
            System.out.println("Lines:" + lineCount + " Elapsed:" + (end - start) + " Fields:" + count);
        printThroughputRate(file, (end - start));
        // 1 Pass: 217180 Elapsed:8309 Fields:115255
        // Pass per root:  217283 Elapsed:51203 Fields:115290
       // }

    }
    private void printThroughputRate(String absolutePath, long elapsed) {
        long length = new File(absolutePath).length();
        double seconds = elapsed/1000.0;
        long mbytes = length/ FileUtil.MEGABYTES;
        System.out.println(String.format("Throughput:%f MB/sec", (mbytes/seconds)));
    }



    @Test
    public void testShouldBuildTwoFields() throws Exception {
        String testLine = " \"key1\": \"value1\",\n \"key2\": \"value2\" crap";
//        List<FieldI> fields = kve.getFields(testLine);
        List<Pair> fields = kve.getFields(testLine);
        assertEquals(2, fields.size());
    }

    @Test
    public void testShouldMisMatchKVUsingTree() throws Exception {
        String testLine = " \"key1\": \"value1\",\n \"key2\": \"value2\" crap";
        JSONObject item = (JSONObject) kve.tree.get(1);
        System.out.println(item);
        KeyValueExtractor.KeyValue kv = kve.nextKeyValue(new KeyValueExtractor.KeyValue(), 0, testLine, item);
        assertFalse(kv.isMatch());
    }


    @Test
    public void testShouldProcessKVUsingTreeAgain() throws Exception {
        String testLine = " CPU:9!9 crap";
        JSONObject item = (JSONObject) kve.tree.get(1);
        System.out.println(item);
        KeyValueExtractor.KeyValue kv = kve.nextKeyValue(new KeyValueExtractor.KeyValue(), 0, testLine, item);
        assertFalse(kv.isMatch());
        String line = "  CPU:100 ";
        KeyValueExtractor.KeyValue kv2 = kve.nextKeyValueScan(new KeyValueExtractor.KeyValue(), 0, line, item);
        assertTrue(kv2.isMatch());
        assertTrue(kv2.toString().contains("CPU:100 pos:10"));
    }
    @Test
    public void testShouldProcessKVUsingForValues() throws Exception {
        JSONObject item = (JSONObject) kve.tree.get(1);

        String line = "2013-04-03 15:59:01,243 INFO tailer-lease-152-2 (log.LogStatsRecorderUpdater)\tLogStats: CPU:100 agentId:alteredcarbon.local-11003-0 host:alteredcarbon.local indexedFileCount:26 currentLiveFiles:26 indexedBytes:9134854 indexTodayBytes:8044500 indexedYesterdayBytes:0";
        //String line = "  CPU:100 stuff:200";
        KeyValueExtractor.KeyValue kv2 = kve.nextKeyValueScan(new KeyValueExtractor.KeyValue(), 0, line, item);
        assertTrue(kv2.isMatch());
        assertTrue("Didnt Match:" + kv2.toString(), kv2.toString().contains("CPU:100 pos:96"));
    }


    @Test
    public void testShouldProcessKVUsingTree() throws Exception {
        String testLine = " key1=\"value1\",\n key2=\"value2\" crap";
        JSONObject item = (JSONObject) kve.tree.get(1);
        System.out.println(item);
        KeyValueExtractor.KeyValue kv = kve.nextKeyValue(new KeyValueExtractor.KeyValue(), 0, testLine, item);
        assertTrue(kv.isMatch());
        assertEquals("kv key1:value1 pos:14", kv.toString());

        KeyValueExtractor.KeyValue kv2 = kve.nextKeyValue(new KeyValueExtractor.KeyValue(), kv.pos, testLine, item);
        assertNotNull(kv2);
        assertEquals("kv key2:value2 pos:30", kv2.toString());

        KeyValueExtractor.KeyValue kv3 = kve.nextKeyValue(new KeyValueExtractor.KeyValue(), kv2.pos, testLine, item);
        assertFalse(kv3.isMatch());
    }

    @Test
    public void testShouldProcessJSONUsingTree() throws Exception {
        String testLine = " \"key1\": \"value1\",\n \"key2\": \"value2\" crap";
        JSONObject item = (JSONObject) kve.tree.get(0);
        System.out.println(item);
        KeyValueExtractor.KeyValue kv = kve.nextKeyValue(new KeyValueExtractor.KeyValue(), 0, testLine, item);
        assertTrue(kv.isDone());
        assertEquals("kv key1:value1 pos:17", kv.toString());

        KeyValueExtractor.KeyValue kv2 = kve.nextKeyValue(new KeyValueExtractor.KeyValue(), kv.pos, testLine, item);
        assertNotNull(kv2);
        assertEquals("kv key2:value2 pos:36", kv2.toString());

        KeyValueExtractor.KeyValue kv3 = kve.nextKeyValue(new KeyValueExtractor.KeyValue(), kv2.pos, testLine, item);
        assertFalse(kv3.isMatch());
    }
    @Test
    public void testShouldProcessJSONNumbers() throws Exception {
        String testLine = "{ \"key1\": 100,\n \"key2\": 200 } crap";
        JSONObject item = (JSONObject) kve.tree.get(0);
        System.out.println(item);
        KeyValueExtractor.KeyValue kv = kve.nextKeyValue(new KeyValueExtractor.KeyValue(), 0, testLine, item);
        assertTrue(kv.isDone());
        assertEquals("kv key1:100 pos:14", kv.toString());

        KeyValueExtractor.KeyValue kv2 = kve.nextKeyValue(new KeyValueExtractor.KeyValue(), kv.pos, testLine, item);
        assertNotNull(kv2);
        assertEquals("kv key2:200 pos:28", kv2.toString());

        KeyValueExtractor.KeyValue kv3 = kve.nextKeyValue(new KeyValueExtractor.KeyValue(), kv2.pos, testLine, item);
        assertFalse(kv3.isMatch());
    }

    @Test
    public void testShouldFindKeyValue() throws Exception {
        String testLine = " \"stuff1\": \"value\"";
        String[] fromSplitTo = new String[] { "\"","\": \"", "\"" };
        String[] from = new String[] { fromSplitTo[0]};
        String[] split = new String[] { fromSplitTo[1]};
        String[] to = new String[] { fromSplitTo[2]};

        KeyValueExtractor.KeyValue kv = kve.nextKeyValue(0, testLine, from, split,  to);
        assertNotNull(kv);
        assertEquals("kv stuff1:value pos:18", kv.toString());
        assertNull(kve.nextKeyValue(kv.pos, testLine, from, split,  to));
    }


    @Test
    public void testNotLocateKey() throws Exception {
        String testLine = " \"stuf-f\": \"value\"";
        KeyValueExtractor.Match match = kve.findMatch(0, testLine, "\"","none");
        assertEquals(true, match.isMatch);
        assertEquals(2, match.pos);

        KeyValueExtractor.Match match2 = kve.findMatch(match.pos, testLine, "\": \"", "alphaNumeric");
        assertEquals(false, match2.isMatch);
        assertEquals(6, match2.pos);
    }

    @Test
    public void testLocateKey() throws Exception {
        String testLine = " \"stuff\": \"value\"";
        KeyValueExtractor.Match match = kve.findMatch(0, testLine, "\"","none");
        assertEquals(true, match.isMatch);
        assertEquals(2, match.pos);

        KeyValueExtractor.Match match2 = kve.findMatch(match.pos, testLine, "\": \"", "alhpaNumeric");
        assertEquals(true, match2.isMatch);
        assertEquals(11, match2.pos);

        // Got the Key
        assertEquals("stuff\": \"", testLine.substring(match.pos, match2.pos) );
    }
}
