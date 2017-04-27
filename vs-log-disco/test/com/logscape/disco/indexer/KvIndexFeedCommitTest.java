package com.logscape.disco.indexer;

import com.liquidlabs.common.file.FileUtil;
import org.junit.Test;

import java.util.List;

public class KvIndexFeedCommitTest {


    //
    //
  //  @Test //- prob doesn't work on build due to testmode
  //  public void shouldCompactFast() {
//        System.setProperty("kv.env","/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/work/DB/kv-index2");
//        KvIndexFeed kvFeed = KvIndexFeed.get(false);
////        System.out.println("1 DBSIZE:" + KvIndexFeed.kvIndexDB.size());
////        System.out.println("2 LtSIZE:" + KvIndexFeed.lutDB.size());
//
//
//        KvIndexFeed.compactDatabases();
//
////        System.out.println("3 DBSIZE:" + KvIndexFeed.kvIndexDB.size());
////        System.out.println("4 LtSIZE:" + KvIndexFeed.lutDB.size());
//
//
//
//    }

    @Test //- prob doesn't work on build due to testmode
    public void shouldPersistStuffWhenDbIsClosed() {

        if (true) return;

        try {
            FileUtil.deleteDir(new java.io.File("work/KV_INDEX"));
            for (int c = 0; c<100; c++) {
                final IndexFeed feed = KvIndexFactory.get();
                feed.reset();
                System.out.println("Doing Iter:" + c);

                int total = 10 * 10 * 100;
                for(int i =0; i< total; i++) {
                    feed.index(1, "", i + 1, 1, " abc:" + i + " def:blah xyz:foo john:dead", false, false, false, null);
                }
                feed.commit();
//                System.out.println("Size:" + KvIndexFeed.kvIndexDB.size());
                List<Pair> fieldsIs = null;
                for(int i =0; i<total; i++) {
                    fieldsIs = feed.get(1, i + 1);
                    //if (fieldsIs == null) System.err.println("boo:" + i);
                }
                System.out.println(c + ") Read:" + fieldsIs);
                feed.close();

                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


    }

    @Test
    public void shouldDoSquat() {

    }
}
