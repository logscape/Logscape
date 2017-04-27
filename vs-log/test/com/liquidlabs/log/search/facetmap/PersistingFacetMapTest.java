package com.liquidlabs.log.search.facetmap;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.search.functions.Function;
import com.logscape.disco.indexer.Pair;
import org.junit.Before;
import org.junit.Test;
import org.prevayler.Prevayler;
import org.prevayler.PrevaylerFactory;
import org.prevayler.foundation.serialization.XStreamSerializer;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by neil on 08/12/15.
 */
public class PersistingFacetMapTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testDirAllocation() throws Exception {
        String dd1 = PersistingFacetMap.getDir("dd", 1000);
        String dd2 = PersistingFacetMap.getDir("dd", 10000);
        String dd3 = PersistingFacetMap.getDir("dd", 100000);
        System.out.println(dd1 + ":" + dd2 + ":" + dd3);
    }


        @Test
    public void testWrite() throws Exception {
        FileUtil.deleteDir(new File("build/persistentStore"));

        PersistingFacetMap store = new PersistingFacetMap("build/persistentStore", "");
        ArrayList<Pair> defaultPairs = new ArrayList<>();
        defaultPairs.add(new Pair("one1", "one1"));

        ArrayList<Pair> discoPairs = new ArrayList<>();
        discoPairs.add(new Pair("disco1", "disco1"));

        store.write("test", 1, defaultPairs, discoPairs, 1);
        store.flush(1);

        PersistingFacetMap store2 = new PersistingFacetMap("build/persistentStore", "");
        Map<String, Map<String, Function>> map = store2.read(1);
        assertEquals(2, map.size());
    }
    @Test
    public void testReading() throws Exception {


        PrevaylerFactory factory = new PrevaylerFactory();
        factory.configureSnapshotSerializer(new XStreamSerializer());
        factory.configureJournalSerializer(new XStreamSerializer());
        factory.configurePrevalentSystem(new HashMap<String, Map<String, Function>>());
        String env = "/Volumes/SSD2/logscape_trunk/LogScape/master/build/logscape/work/DB/summary";
        int logId = 6;
        String prevalenceDirectory = env + "/" + logId + ".sum";
        factory.configurePrevalenceDirectory(prevalenceDirectory);
        Prevayler prevayler = factory.create();
        PersistentMap<Object, Object> functionsMap = new PersistentMap<>(prevayler);
        Object level = functionsMap.get("level");
        System.out.println(level);


    }


}