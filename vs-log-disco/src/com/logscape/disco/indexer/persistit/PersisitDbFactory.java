package com.logscape.disco.indexer.persistit;

import com.liquidlabs.common.collection.CompactCharSequence;
import com.liquidlabs.common.collection.cache.ConcurrentLRUCache;
import com.liquidlabs.common.file.FileUtil;
import com.logscape.disco.DiscoProperties;
import com.logscape.disco.indexer.*;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Value;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.KeyCoder;
import com.persistit.encoding.ValueCoder;
import com.persistit.exception.ConversionException;
import com.persistit.exception.PersistitException;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 26/11/2013
 * Time: 16:10
 * To change this template use File | Settings | File Templates.
 */
public class PersisitDbFactory {

    static String env = System.getProperty("kv.env", DiscoProperties.getKVIndexDB());

    private static Persistit persistit = null;
    static Dictionary dictionary = null;
    public static void copyConfigFile() {

        String piConfig = System.getProperty("kvi.persistit.config", env + "/kvi.persistit.properties");
        if (new File(piConfig).exists()) {
            System.err.println("Schema Change - removing existing kvi.persisti.properties File (for replacement)");
            new File(piConfig).delete();
        }
    }

    synchronized public static Persistit  getPersistit() {
        if (persistit == null) {
            try {

                if (DiscoProperties.isForwarder) {
                    System.setProperty("buffer.memory.2048","512K,20G,64M,0.2");

                }
                final Persistit db = new Persistit();
                String runtimeConfigFile = System.getProperty("kvi.persistit.config", env + "/kvi.persistit.properties");
                File distributedConfigFile = new File(System.getProperty("kvi.default.pi.config", "./downloads/kvi.persistit.properties"));
                boolean testMode = !new File(runtimeConfigFile).exists();
                if (!new File(runtimeConfigFile).exists() && distributedConfigFile.exists()) {
                    System.err.println("Persistit: Failed to find existing runtime Config - writing new one");
                    System.err.println("Persistit: Copying:" + distributedConfigFile + " -> " + runtimeConfigFile);
                    FileUtil.copyFile(distributedConfigFile, new File(runtimeConfigFile));
                    testMode = false;
                }



                if (testMode) {
                    System.err.println("Persistit Test MODE:" + new File(".").getAbsolutePath());
                    FileUtil.deleteDir(new File("./build/DB/"));
                    new File("./build/DB/kv-index/data").mkdirs();
                    new File("./build/DB/kv-index/journal").mkdirs();
                    db.setPropertiesFromFile("persistit.properties");

                } else {
                    new File("./work/DB/kv-index/data").mkdirs();
                    new File("./work/DB/kv-index/journal").mkdirs();
                    db.setPropertiesFromFile(runtimeConfigFile);

                }
                db.initialize();
                db.getCoderManager().registerKeyCoder(TupleIntInt.class, new FunTuple2KeyCoder());
                persistit = db;
                new File("./work/DB/kv-index/" + KvIndexFeed.REBUILD_DB).delete();
            } catch (PersistitException piex) {
                piex.printStackTrace();
                throw new RuntimeException(piex);
            } catch (Throwable piex) {
                piex.printStackTrace();
                throw new RuntimeException(piex);
            }
        }
        return persistit;
    }

    public static Db<Integer, String[]> getLutThreadedDb() {
        try {
            getPersistit();

            return new PersistitThreadedDb<Integer, String[]>("lut","lut", persistit, DBThreadLocalLUT.threadLocal, !KvIndexFactory.readOnly);
        } catch (Exception ex) {
            throw new RuntimeException("ex:", ex);
        }
    }


    public static Db<TupleIntInt, int[]> getIndexThreadedDb() {

        getPersistit();
//                Properties prop = new Properties();
//                String value = env + "/" + dbName;
//                new java.io.File(value).mkdirs();
//                System.out.println("KvIndex:" + new java.io.File(value).getAbsolutePath());
//                prop.put("datapath", value);
//                prop.put("buffer.memory." + KVI_PAGE_SIZE_K * 1024, KVI_BUFFERS_CONFIG);
//                prop.put("logfile","${datapath}/" + dbName + ".log");
//                String pageSetup = "pageSize: " + KVI_PAGE_SIZE_K + "K,initialSize:10M,extensionSize:10M,maximumSize:500G";
//                String action = KvIndexFeed.readOnly ? "readOnly" : "create";
//                prop.put("volume.1","${datapath}/" + dbName + "," + action + "," + pageSetup);
//                prop.put("journalpath","${datapath}/" + dbName);
//
//                Persistit persistit = new Persistit(prop);
//                persistit.getCoderManager().registerKeyCoder(TupleIntInt.class, new FunTuple2KeyCoder());
//                indexDB = persistit;
        return new PersistitThreadedDb<TupleIntInt, int[]>("index", "index", persistit, DBThreadLocalKVI.threadLocal, !KvIndexFactory.readOnly);
    }

    public static Dictionary getDictionary() {

        try {
            if (dictionary == null) {
                getPersistit();
                dictionary = new DictionaryImpl("dictionary", persistit, DBThreadLocalDict.threadLocal, !KvIndexFactory.readOnly);
            }
            return dictionary;
        } catch (Throwable t) {
            throw new RuntimeException("ex:", t);
        }
    }

    public static class DBThreadLocalLUT {
        public static final ThreadLocal threadLocal = new ThreadLocal();
    }
    public static class DBThreadLocalDict {
        public static final ThreadLocal threadLocal = new ThreadLocal();
    }
    public static class DBThreadLocalKVI {
        public static final ThreadLocal threadLocal = new ThreadLocal();
    }


    // From here:
    // https://github.com/pdbeaman/persistit/blob/master/doc/BasicAPI.rst
    public static class FunTuple2KeyCoder implements KeyCoder {

        @Override
        public void appendKeySegment(Key key, Object object, CoderContext context) throws ConversionException {
            TupleIntInt ff = (TupleIntInt) object;
            key.append(ff.a);
            key.append(ff.b);
        }

        @Override
        public Object decodeKeySegment(Key key, Class<?> clazz, CoderContext context) throws ConversionException {
            int a = key.decodeInt();
            int b = key.decodeInt();
            return new  TupleIntInt(a,b);
        }

        @Override
        public boolean isZeroByteFree() throws ConversionException {
            return false;
        }
    }

    private static class CompactCharSequenceCoder implements ValueCoder {

        ConcurrentLRUCache<String, CompactCharSequence> dedupCache;
        public CompactCharSequenceCoder(ConcurrentLRUCache<String, CompactCharSequence> dedupCache) {
            this.dedupCache = dedupCache;
        }
        @Override
        public void put(Value value, Object object, CoderContext context) throws ConversionException {
            CompactCharSequence cc = (CompactCharSequence) object;
            value.putByteArray(cc.data());
        }

        @Override
        public Object get(Value value, Class<?> clazz, CoderContext context) throws ConversionException {
            byte[] data = value.getByteArray();
            CompactCharSequence ccs = new CompactCharSequence(data);
            String key = ccs.toString();
            CompactCharSequence existing = dedupCache.get(key);
            if (existing != null) {
                return existing;
            }
            else dedupCache.put(key, ccs);
            return ccs;
        }
    }
    public static void close() {
        try {
            if (persistit != null) persistit.close();
        } catch (PersistitException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        persistit = null;
    }
}
