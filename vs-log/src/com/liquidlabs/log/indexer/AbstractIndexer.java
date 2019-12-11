package com.liquidlabs.log.indexer;

import com.liquidlabs.common.TestModeSetter;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.BucketKey;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFileOps;
import com.logscape.disco.indexer.*;
import com.logscape.disco.indexer.persistit.PersisitDbFactory;
import com.logscape.disco.indexer.persistit.PersistitThreadedDb;
import com.persistit.Persistit;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
//import com.twitter.prestissimo.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 03/12/2013
 * Time: 16:43
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractIndexer implements Indexer {
    private static final Logger LOGGER = Logger.getLogger(AbstractIndexer.class);

    /**
     * Performance Notes:
     * Addendum to previous message. I forgot to mention that the starting point for this discussion was buffer pool size.
     * In my tests the allocation was set for 5,000 buffers for 80MBytes of memory.
     * This seems like a modest amount of memory to allocate in a 1 Gbyte heap (from the -Xmx setting in the pom).
     * I tried it also at 10,000 and 25,000 buffers and that didn't make much difference.
     * But in general we recommend allocating a significant amount amount of memory space for buffers,
     * and 32 buffers and even 1,000 would be quite inadequate for your use case.
     * Once the buffer pool is full of dirty buffers, we have no choice but to flush them even if the
     * very next operation will further modify one of the pages in the pool.  A larger pool generally allows
     * many more changes to occur within each page before it must be flushed to disk.  Likewise, it typically
     * allows each page to serve many more read operations before being replaced by a different page.
     */

    protected Persistit persistit = null;
//    private static Prestissimo persistit = null;
    static int initialPages = Integer.getInteger("pi.page.initial", 50);
    static Boolean readOnly =  Boolean.getBoolean("log.db.readonly");

    public Db<String, byte[]> getStore(String env, String dbName, ThreadLocal threadLocal) {

        try {
            createPI(env);
            return new PersistitThreadedDb<String, byte[]>(dbName, dbName, persistit, threadLocal, !readOnly);
//            return new PrestoThreadedDb<String, byte[]>(dbName, dbName, persistit, threadLocal, !readOnly);
        } catch (Exception ex) {
            ex.printStackTrace();;
            throw new RuntimeException("ex:", ex);
        }
    }



    public Db<BucketKey, Bucket> getStoreCKey(String env, String dbName, ThreadLocal threadLocal) {

        try {
            createPI(env);

//            return new PrestoThreadedDb<BucketKey, Bucket>(dbName, dbName, persistit, threadLocal, !readOnly);
            return new PersistitThreadedDb<BucketKey, Bucket>(dbName, dbName, persistit, threadLocal, !readOnly);
        } catch (Exception ex) {
            throw new RuntimeException("ex:", ex);
        }
    }
    static {
        if ( TestModeSetter.isTestMode()) {
            System.setProperty("index.page.size.k","4");
            System.setProperty("index.buffers","1M,1G,4M,0.02");
        }
    }


    private void createPI(String env) throws Exception {
//        if (persistit != null) return;
        persistit = PersisitDbFactory.getPersistit();
        persistit.getCoderManager().registerKeyCoder(BucketKey.class, new BucketKey.KeyCoder());
        persistit.getCoderManager().registerValueCoder(Bucket.class, new Bucket.ValueCoder());
//        persistit = PrestoDbFactory.getPersistit();
//        persistit.getCoderManager().registerKeyCoder(BucketKey.class, new BucketKey.KeyCoder());
//        persistit.getCoderManager().registerValueCoder(Bucket.class, new Bucket.ValueCoder());

    }



    static public boolean isSchemaRebuildRequired(String environment)  {
        if (TestModeSetter.isTestMode()) return false;
        String existingSchemaVersion = getExistingSchemaVersion(environment);
        LOGGER.info(String.format("Validate Schema [%s] existing[%s] env[%s]", SCHEMA_VERSION, existingSchemaVersion, environment));
        if (!new File(environment).exists()) {
            LOGGER.info("DB Env is Empty:"+ environment);
        } else if (!SCHEMA_VERSION.equals(existingSchemaVersion)) {
            LOGGER.info(String.format("Schema mismatch this[%s] found[%s]", SCHEMA_VERSION, existingSchemaVersion));
            File dir = new File(environment + "/..");
            int i = FileUtil.deleteDir(dir);
            LOGGER.info("Deleted ./DB files:" + i);
            if (i > 0) {
                LOGGER.info("Confirming/Retrying for locked files");
                for (int c = 0; c < 5; c++) {
                    i = FileUtil.deleteDir(dir);
                    if (i > 0) {
                        LOGGER.info("Deleted ./DB files:" + i);
                        try {
                            Thread.sleep(10 * 1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                    }

                }
            }

            writeSchema(environment);
            return true;
        }
        writeSchema(environment);
        return false;
    }

    private static void writeSchema(String environment) {
        new File(environment).mkdirs();
        try {
            LOGGER.info("Writing DBSchema File, Version:" + SCHEMA_VERSION);
            FileOutputStream fos = new FileOutputStream(environment + DB_SCHEMA_ID);
            fos.write(SCHEMA_VERSION.getBytes());
            fos.close();

        } catch (IOException e) {
            LOGGER.error("SchemaCreateFailed", e);
        }
    }

    public static void rebuildIndex(String environment) {
        // failed to open the indexer environment.... rebuilt the DB?
        new File(environment+DB_SCHEMA_ID).delete();

        LOGGER.info("Rebuild DB:" + environment + "/..");
        KvIndexFeed.rebuildIndex();
        File dir = new File(environment + "/..");
        LOGGER.info("Deleted ./DB files:" + FileUtil.deleteDirWithRetry(dir));

        writeSchema(environment);
    }


    static private String getExistingSchemaVersion(String envHome) {
        try {
            FileInputStream fis = new FileInputStream(envHome + DB_SCHEMA_ID);
            byte[] buffer = new byte[fis.available()];
            fis.read(buffer);
            fis.close();
            return new String(buffer);
        } catch (Exception e) {
            LOGGER.info("Failed to read schema-id:" + e.toString());
        }
        return "";
    }


}
