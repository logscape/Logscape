package com.logscape.disco.indexer.mapdb;

import com.liquidlabs.common.file.FileUtil;
import com.logscape.disco.indexer.IndexFeed;
import com.logscape.disco.indexer.KvIndexFactory;
import com.logscape.disco.indexer.Pair;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 17/09/2013
 * Time: 15:30
 * To change this template use File | Settings | File Templates.
 */
public class KvIndexFeedFunctionalTest {

    
    @Before
    public void setUp() throws Exception {
        File dir = new File("build/work/DB");
        FileUtil.deleteDir(dir);
        dir.mkdirs();

    }

    @Test
    public void shouldDiscoverStoreAndGetLineFields() throws Exception {
        IndexFeed feed = KvIndexFactory.get(KvIndexFactory.IMPLS.MAPDB);
        String sourceLine = "2013-09-18 10:29:11,702 INFO pool-2-thread-1 (license.TrialListener)\tCPU:99 Action:'Download' Email:'izam.my@gmail.com' IpAddress:'175.143.7.68' Company:'sadasd'";
        System.out.println(sourceLine);
        List<Pair> crap = feed.index(1, "crap", 1, System.currentTimeMillis(), sourceLine, true, true, true, null);
        feed.store(1, 1, crap);
        Map<String,String> asMap = feed.getAsMap(1, 1, System.currentTimeMillis());

        assertTrue(asMap.size() > 1);
        System.out.println("Fields:" + asMap.toString().replace(", ", "\n"));
        assertEquals("99", asMap.get("CPU"));
        assertEquals("Download", asMap.get("Action"));

    }

    @Test
    public void shouldRemoveIt() throws Exception {
        IndexFeed feed = KvIndexFactory.get(KvIndexFactory.IMPLS.MAPDB);
        String sourceLine = "2013-09-18 10:29:11,702 INFO pool-2-thread-1 (license.TrialListener)\tCPU:99 Action:'Download' Email:'izam.my@gmail.com' IpAddress:'175.143.7.68' Company:'sadasd'";
        System.out.println(sourceLine);

        int logId = 100;
        long time = -1;
        List<Pair> crap = feed.index(logId, "crap", 1, time, sourceLine, true, true, true, null);
        feed.store(logId, 1, crap);
        Map<String,String> asMap = feed.getAsMap(logId, 1, time);

        assertTrue(asMap.containsKey("CPU"));

        feed.remove(logId);
        Map<String,String> asMap2 = feed.getAsMap(logId, 1, time);
        assertTrue(asMap2.size() == 0);

    }

}
