package com.logscape.play;

import com.liquidlabs.common.concurrent.ExecutorService;
//import com.liquidlabs.log.search.ReplayEvent;
//import com.logscape.play.replay.PerRequestSessionDb;
import junit.framework.Assert;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Neiil on 10/28/2015.
 */
public class PerRequestSessionDbTest {

//    @Test
//    public void shouldGetCreated() {
//        File file = new File("build", "DB_ID");
//        file.delete();
//
//        DB db = DBMaker.newFileDB(file)
//                .transactionDisable()
//                .cacheWeakRefEnable().make();
//
//        PerRequestSessionDb per = new PerRequestSessionDb("test", db, ExecutorService.newScheduledThreadPool("1"));
//        ReplayEvent event = new ReplayEvent("source", 100, 100, 1, "sub", System.currentTimeMillis(), "(*)", "line data");
//
//        per.addEvents(new ArrayList(Arrays.asList(event)));
//        ReplayEvent gotitBack = per.events().get(event.getId());
//        Assert.assertEquals(event, gotitBack);
//
//
//
//    }


}
