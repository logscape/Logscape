package com.logscape.disco.indexer.flatfile;

import com.liquidlabs.common.file.FileUtil;
import com.logscape.disco.DiscoProperties;
import com.logscape.disco.grokit.GrokItPool;
import com.logscape.disco.indexer.IndexFeed;
import com.logscape.disco.indexer.Pair;
import com.logscape.disco.kv.KVExtractor;
import com.logscape.disco.kv.RulesKeyValueExtractor;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 06/01/15
 * Time: 22:15
 * To change this template use File | Settings | File Templates.
 */

public class DataStreamIndexFeed implements IndexFeed {

    private static final Logger LOGGER = Logger.getLogger(IndexFeed.class);
    private static boolean LOGMISS = Boolean.getBoolean("log.disco.dsteam.logmiss");

    private String env;
    private KVExtractor kvExtractor;
    private final GrokItPool grokIt;


    public DataStreamIndexFeed(String env, RulesKeyValueExtractor kvExtractor, GrokItPool grokitPool) {
        FileUtil.mkdir(env);
        this.env = env;
        this.kvExtractor = kvExtractor;
        this.grokIt = grokitPool;
    }

    @Override
    public KVExtractor kvExtractor() {
        return kvExtractor;
    }


    public List<Pair> index(int logId, String filename, int lineNo, long timeMs, String data, boolean isFieldDiscoveryEnabled, boolean grokDiscoveryEnabled, boolean isSystemFieldsEnbled, List<Pair> indexedFields){
        List<Pair> fields = new ArrayList<Pair>();
        if (isFieldDiscoveryEnabled) {
            fields.addAll(kvExtractor.getFields(data));
        }

        if (grokDiscoveryEnabled) {
            addFields(fields, grokIt.processLine(filename, data));
        }
        if (isSystemFieldsEnbled) {
            fields.add(new Pair("_timestamp",Integer.toString(DiscoProperties.fromMsToSec(timeMs))));
        }

        return fields;
    }
    private void addFields(List<Pair> fields, Map<String, String> grokFields) {
        boolean added = false;
        for (Map.Entry<String, String> entry : grokFields.entrySet()) {
            String value = entry.getValue();
            String name = entry.getKey();
            added = false;

            for (Pair field : fields) {
                if (field.key.equals(name)) {
                    field.value = value;
                    added = true;
                }
            }
            if (!added) {
                fields.add(new Pair(name, value));
            }
        }
    }



    int currentLogId = -1;
    DataOutputStream os = null;
    public void store(int id, int line, List<Pair> discovered){

        if (os == null || currentLogId != id) {
            if (os != null) {
                try {
                    os.close();

                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
            File outfile = getFile(id);
            System.out.println("OutFile" + outfile.getAbsolutePath());
            try {
                os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outfile)));
                currentLogId = id;
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Cannot Create IDX file" + new File(env).getAbsolutePath(), e);
            }
        }


        try {

            os.writeInt(discovered.size());
            for (Pair pair : discovered) {
                os.writeUTF(pair.key);
                os.writeUTF(pair.value);
            }
            os.flush();

        } catch (IOException e) {
            throw new RuntimeException("Cannot Create IDX file" + new File(env).getAbsolutePath(), e);
        }
    }

    private File getFile(int id) {
        return new File(env, id + ".dstr");
    }


    int inLine = -1;
    int inLogId = -1;
    DataInputStream is = null;
    boolean bailingout = false;

    public Map<String,String> getAsMap(int logId, int lineNo, long timeMs){

        if (bailingout) return Collections.EMPTY_MAP;
        if (is == null || inLogId != logId || inLine > lineNo){
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    return Collections.EMPTY_MAP;
                }
            }
            try {
                File file = getFile(logId);
                if (!file.exists()) return Collections.EMPTY_MAP;
                is = new DataInputStream(new BufferedInputStream(new FileInputStream(file.getAbsolutePath()), 4 * 64 * 1024));
                inLine = 0;
                inLogId = logId;
            } catch (IOException e) {
                throw new RuntimeException("FileNotFound Reading KV:" + " LogID:" + logId + " line:" + lineNo + " Time:" + new DateTime(timeMs), e);
            }
        }

        Map<String, String> collecting = null;
        try {

            // expected
            if (inLine == lineNo -1) {
                collecting = readObj(is);
                inLine++;
            } else {
                // scroll
                while (inLine < lineNo) {
                    if (LOGMISS) LOGGER.warn("MISS:" + logId + " line:" + inLine + "<" + lineNo);
                    collecting = readObj(is);
                    inLine++;
                }
                if (LOGMISS) LOGGER.warn("DONE:" + logId + " " + collecting.toString());
            }
            return  collecting;

        } catch (Exception e) {
            throw new RuntimeException("StreamFailed Reading KV LogID:" + logId + " line:" + lineNo + " Time:" + new DateTime(timeMs), e);
        }
    }

    private Map<String, String> readObj(DataInputStream is) {

        try {
            int items = 0;

//            if (is.available() == 0) return Collections.EMPTY_MAP;
            try {
                items = is.readInt();
            } catch (Throwable t) {
                bailingout = true;
                return  Collections.EMPTY_MAP;
            }
            HashMap<String, String> results = new HashMap<String, String>();
            for (int i = 0; i < items; i++) {
                results.put(is.readUTF(),is.readUTF());
            }

            return results;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read from Stream");
        }
    }

    public List<Pair> get(int logId, int lineNo){
        return null;

    }

    public void remove(int logId){
        closeInputStream();
        closeOutputStream();
        getFile(logId).delete();

    }
    public void commit(){
        closeOutputStream();
        closeInputStream();
    }

    public void compact(){
        closeInputStream();
        closeOutputStream();
    }
    public void close(){
        closeOutputStream();
        closeInputStream();
    }
    private void closeOutputStream() {
        try {
            if (os != null) {
                os.flush();
                os = null;
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private void closeInputStream() {
        try {
            if (is!= null) {
                is.close();
                is = null;
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public boolean isRebuildRequired(){
        return false;
    }

    public void setKvExtractor(KVExtractor kve){
        this.kvExtractor = kve;
    }

    public void reset() {
    }
}
