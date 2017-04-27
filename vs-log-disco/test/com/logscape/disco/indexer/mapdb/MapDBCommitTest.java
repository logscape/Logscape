package com.logscape.disco.indexer.mapdb;

import com.liquidlabs.common.collection.Arrays;
import com.logscape.disco.indexer.mapdb.StringArraySerialiser;
import org.junit.Test;
import org.mapdb.*;

import java.io.File;

public class MapDBCommitTest {

    public static final String TREEMAP_NAME = "tree";
    public static String DB_NAME = "LUT";
    BTreeKeySerializer keySerializer = BTreeKeySerializer.TUPLE2;
    //BTreeKeySerializer keySerializer = BTreeKeySerializer.STRING;
    Serializer valueSerializer = new StringArraySerialiser();
    String directory = "build/KV";

    boolean isOffHeap = false;

    @Test
    public void shouldPersistStuffWhenDbIsClosed() {
        if (true) return;
        DB_NAME += keySerializer.getClass().getSimpleName();

        cleanupDir(directory);


        DB db0 = getDbFOR(directory, DB_NAME, TREEMAP_NAME, keySerializer, valueSerializer);

        BTreeMap<Fun.Tuple2, String[]> tree = db0.getTreeMap(TREEMAP_NAME);
        //BTreeMap<String, String[]> tree = db0.getTreeMap("tree");


        int total = 10 * 10000;
        for (int i = 0; i < total; i++) {
            tree.put(Fun.t2(1, 1 + i), ("abc:" + i + " def:blah xyz:foo john:dead").split(" "));
            //tree.put(i + "", ("abc:" + i + " def:blah xyz:foo john:dead").split(" "));
            //db0.commit();
        }

        db0.commit();

        System.out.println("Persisted:" + tree.size());
        db0.close();

        System.out.println("Loading:");

        for (int i = 0; i < 100; i++) {
            DB db1 = getDbFOR(directory, DB_NAME, TREEMAP_NAME, keySerializer, valueSerializer);

            //BTreeMap<String, String[]> tree2 = db1.getTreeMap(TREEMAP_NAME);
            BTreeMap<Fun.Tuple2, String[]> tree2 = db1.getTreeMap(TREEMAP_NAME);
            String[] value = tree2.get(Fun.t2(1, 1 + 1));
            //String[] value = tree2.get("1");
            System.out.println("GOT Item:" + Arrays.toString(value));

            int size = tree2.keySet().size();
            System.out.println("GotKeys:" + size);

            for (int ii = 0; ii < total; ii++) {
                tree2.put(Fun.t2(1, 1 + ii), ("abc:" + ii + " def:blah xyz:foo john:dead").split(" "));
                //tree2.put(i + "", ("abc:" + i + " def:blah xyz:foo john:dead").split(" "));

            }
            db1.commit();
            db1.close();
        }


    }

    private void cleanupDir(String directory) {
        File[] files = new File(directory).listFiles();
        int deleted = 0;
        if (files != null) {
            for (File file : files) {
                file.delete();
                deleted++;
            }
        }
        System.out.println("Cleanup Items:" + deleted);
        new File(directory).mkdirs();
    }

    private DB getDbFOR(String environmentDir, String dbName, String treemapName, BTreeKeySerializer keySerializer, Serializer valueSerializer) {
        new File(environmentDir).mkdirs();
        DB db = getDb(environmentDir, dbName)
                .transactionDisable()
                .cacheLRUEnable()
                .cacheWeakRefEnable()
                .closeOnJvmShutdown()
                .make();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println(">>>>>>>>>>>> Shutdown");
                super.run();    //To change body of overridden methods use File | Settings | File Templates.
            }
        });

        if (!db.exists(treemapName)) {
            System.out.println("Creating TREE-MAP:" + treemapName);
            DB.BTreeMapMaker bTreeMapMaker = db.createTreeMap(treemapName).nodeSize(32);
            if (keySerializer != null) bTreeMapMaker = bTreeMapMaker.keySerializer(keySerializer);
            if (valueSerializer != null) bTreeMapMaker = bTreeMapMaker.valueSerializer(valueSerializer);
            bTreeMapMaker.make();
        } else {
            System.out.println("Existing TREE-MAP:" + treemapName);
        }
        return db;
    }

    private DBMaker getDb(String environmentDir, String name) {

        final DBMaker dbMaker = DBMaker.newFileDB(new File(environmentDir, name));
        return dbMaker;
    }

    private static boolean isWindows() {
        return System.getProperty("os.name").toUpperCase().contains("WINDOW");
    }
}
