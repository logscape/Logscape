package com.logscape.disco.indexer.flatfile;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.transport.serialization.Convertor;
import com.logscape.disco.DiscoProperties;
import com.logscape.disco.grokit.GrokItPool;
import com.logscape.disco.indexer.IndexFeed;
import com.logscape.disco.indexer.Pair;
import com.logscape.disco.kv.KVExtractor;
import com.logscape.disco.kv.RulesKeyValueExtractor;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 06/01/15
 * Time: 22:15
 * To change this template use File | Settings | File Templates.
 */

public class BIIndexFeed implements IndexFeed {

    public static final int BYTESLENGTH = 3;
    private String env;
    private KVExtractor kvExtractor;
    private final GrokItPool grokIt;

    public BIIndexFeed(String env, RulesKeyValueExtractor kvExtractor, GrokItPool grokitPool) {
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
        if (indexedFields != null) fields.addAll(indexedFields);
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
    OutputStream os = null;
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
                os = new BufferedOutputStream(new FileOutputStream(outfile));
                currentLogId = id;
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Cannot Create IDX file" + new File(env).getAbsolutePath(), e);
            }
        }


        try {
            Map<String, String> binary = new HashMap<String, String>();
            binary.put(FIELDS._line.name(),line+"");
            for (Pair pair : discovered) {
                binary.put(pair.key, pair.value);
            }

            byte[] serialize = Convertor.serialize(binary);
            byte[] intBytes = Convertor.serialize(new Integer(serialize.length));
            os.write(intBytes);
            os.write(serialize);

        } catch (IOException e) {
            throw new RuntimeException("Cannot Create IDX file" + new File(env).getAbsolutePath(), e);
        }
    }

    private File getFile(int id) {
        return new File(env, id + ".bidx");
    }


    int inLine = -1;
    int inLogId = -1;
    InputStream is = null;
    HashMap<String, String> nothing  = new HashMap<String, String>();

    public Map<String,String> getAsMap(int logId, int lineNo, long timeMs){


        if (is == null || inLogId != logId || inLine != lineNo-1){
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    return nothing;
                }
            }
            try {
                File file = getFile(logId);
                if (!file.exists()) return nothing;
                is = new BufferedInputStream(new FileInputStream(file.getAbsolutePath()), 4 * 64 * 1024);
                inLine = 0;
                inLogId = logId;
            } catch (IOException e) {
                e.printStackTrace();
                return nothing;
            }
        }

        HashMap<String, String> collecting = null;
        try {

            // expected
            if (inLine == lineNo -1) {
                collecting = readObj(is);
                inLine++;
            } else {
                // scroll
                while (inLine < lineNo) {
                    collecting = readObj(is);
                    inLine++;
                }
            }
            return  collecting;

        } catch (Exception e) {
            System.err.println("DOH!");
            e.printStackTrace();
        }
        return collecting;

    }

    private HashMap<String, String> readObj(InputStream is) {

        byte[] intByes = new byte[BYTESLENGTH];
        try {
            is.read(intByes);
            Integer length = (Integer) Convertor.deserialize(intByes);
            byte[] bytes2 = new byte[length];
            is.read(bytes2);
            return (HashMap<String, String>) Convertor.deserialize(bytes2);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ClassNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    public List<Pair> get(int logId, int lineNo){
        return null;

    }

    public void remove(int logId){
        getFile(logId).delete();

    }
    public void commit(){
        try {
            if (os != null) os.flush();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }
    public void compact(){

    }
    public void close(){
        try {
            os.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        try {
            is.close();
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
