package com.logscape.disco.indexer.flatfile;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.file.FileUtil;
import com.logscape.disco.DiscoProperties;
import com.logscape.disco.grokit.GrokItPool;
import com.logscape.disco.indexer.IndexFeed;
import com.logscape.disco.indexer.Pair;
import com.logscape.disco.kv.KVExtractor;
import com.logscape.disco.kv.RulesKeyValueExtractor;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 06/01/15
 * Time: 22:15
 * To change this template use File | Settings | File Templates.
 */

public class BArrayIndexFeed implements IndexFeed {

    private String env;
    private KVExtractor kvExtractor;
    private final GrokItPool grokIt;


    public BArrayIndexFeed(String env, RulesKeyValueExtractor kvExtractor, GrokItPool grokitPool) {
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

    private static byte int3(int x) { return (byte)(x >> 24); }
    private static byte int2(int x) { return (byte)(x >> 16); }
    private static byte int1(int x) { return (byte)(x >>  8); }
    private static byte int0(int x) { return (byte)(x      ); }


    /**
     * output stuff
     */
    int currentLogId = -1;
    FileOutputStream os = null;
    byte[] outBuffer = new byte[16 * 1024];
    int outPos;
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
                os = new FileOutputStream(outfile);
                currentLogId = id;
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Cannot Create IDX file" + new File(env).getAbsolutePath(), e);
            }
        }


        try {

            int payload =getByteArraySize(discovered);
            outBuffer[outPos++] = int0(payload);
            outBuffer[outPos++] = int1(payload);
            outBuffer[outPos++] = int2(payload);
            outBuffer[outPos++] = int3(payload);
            outBuffer[outPos++] = (byte)(discovered.size());

            for (Pair pair : discovered) {
                writeString(pair.key.toCharArray());
                writeString(pair.value.toCharArray());
            }
            os.write(outBuffer, 0, outPos);
            outPos = 0;

        } catch (IOException e) {
            throw new RuntimeException("Cannot Create IDX file" + new File(env).getAbsolutePath(), e);
        }
    }

    private int getByteArraySize(List<Pair> discovered) {
        int result = 1;
        for (Pair pair : discovered) {
            result+=pair.key.length()+1;
            result+=pair.value.length()+1;
        }
        return result;
    }

    private void writeString(char[] chars) {
        outBuffer[outPos++] = (byte) chars.length;
        for (int i = 0; i < chars.length; i++) {
            outBuffer[outPos++] = (byte) chars[i];
        }
    }

    private File getFile(int id) {
        return new File(env, id + ".bbIdx");
    }


    static private int makeInt(byte b0, byte b1, byte b2, byte b3) {
        return (((b3       ) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) <<  8) |
                ((b0 & 0xff)      ));
    }

    int inLine = -1;
    int inLogId = -1;
    FileInputStream is = null;
    byte[] inBuffer = new byte[4 * 1024];
    int inPos = 0;

    public Map<String,String> getAsMap(int logId, int lineNo, long timeMs){


        if (is == null || inLogId != logId || inLine != lineNo-1){
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
                is = new FileInputStream(file.getAbsolutePath());


                inLine = 0;
                inLogId = logId;
            } catch (IOException e) {
                e.printStackTrace();
                return Collections.EMPTY_MAP;
            }
        }


        try {

            inPos = 0;

            HashMap<String, String> collecting = null;

            // expected
            if (inLine == lineNo -1) {
                collecting = readObj();
                inLine++;
            } else {
                // scroll
                while (inLine < lineNo) {
                    collecting = readObj();
                    inLine++;
                }
            }
            return  collecting;

        } catch (Exception e) {
            System.err.println("DOH!");
            e.printStackTrace();
        }
        return Collections.EMPTY_MAP;

    }

    private HashMap<String, String> readObj() {

        int readAmount = 0;
        try {
            HashMap<String, String> results = new HashMap<String, String>();

            // read the size bytes
            inPos = 0;
            is.read(inBuffer,0, 4);

            int amountToRead = makeInt(inBuffer[inPos++],inBuffer[inPos++],inBuffer[inPos++],inBuffer[inPos++]);
            readAmount = amountToRead;

            // read fresh
            inPos = 0;
            is.read(inBuffer,0, amountToRead);

            int discoveredItems = inBuffer[inPos++];
            for (int i = 0; i < discoveredItems; i++) {
                String key = readString(inBuffer);
                String value = readString(inBuffer);
                results.put(key, value);
            }
            return results;
        } catch (IOException e) {
            System.err.println("AM:" + readAmount);
            e.printStackTrace();
        }

        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    private String readString(byte[] inBuffer) {
        int length = inBuffer[inPos++];
        char[] contents = new char[length];
        for (int i = 0; i < length; i++) {
            contents[i] = (char) inBuffer[inPos++];
        }
        return StringUtil.wrapCharArray(contents);
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
