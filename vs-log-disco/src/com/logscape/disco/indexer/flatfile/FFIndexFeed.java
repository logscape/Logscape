package com.logscape.disco.indexer.flatfile;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.logscape.disco.DiscoProperties;
import com.logscape.disco.SystemIndexer;
import com.logscape.disco.grokit.GrokItPool;
import com.logscape.disco.indexer.IndexFeed;
import com.logscape.disco.indexer.Pair;
import com.logscape.disco.kv.KVExtractor;
import com.logscape.disco.kv.RulesKeyValueExtractor;
import org.apache.log4j.Logger;

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

public class FFIndexFeed implements IndexFeed {

    private final static Logger LOGGER = Logger.getLogger(FFIndexFeed.class);

    private String env;
    private KVExtractor kvExtractor;
    private final GrokItPool grokIt;
    private long lastWrite;

    public FFIndexFeed(String env, RulesKeyValueExtractor kvExtractor, GrokItPool grokitPool) {
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
            fields.add(new Pair("_datetime", DateUtil.shortDateTimeFormat6.print(timeMs)));
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

        lastWrite = System.currentTimeMillis();
        if (os == null || currentLogId != id) {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            File outfile = getFile(id);
            if (line == 1 && outfile.exists()) {
                outfile.delete();
            }

            try {
                os = new BufferedOutputStream(new FileOutputStream(outfile, true));
                currentLogId = id;
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Cannot Create IDX file" + new File(env).getAbsolutePath(), e);
            }
        }

        try {
            StringBuilder sb = new StringBuilder();
            new Pair(FIELDS._line.name(), line+"").toFlatFile(sb);
            for (Pair pair : discovered) {
                pair.toFlatFile(sb);
            }
            sb.append("\n");


            os.write(sb.toString().getBytes());
        } catch (IOException e) {
            throw new RuntimeException("Cannot Create IDX file" + new File(env).getAbsolutePath(), e);
        }
    }

    private File getFile(int id) {
        return new File(env, id + ".idx");
    }


    int inLogId = -1;
    RAF raf = null;
    static HashMap<String, String> nothing  = new HashMap<String, String>();

    static String[] kvs = "line!18!device_id!WEST_STREET!duration!59!policy_id!1!service!icmp!proto!1!zone!Trust!action!Permit!sent!102!rcvd!0!src!10.181.100.87!dst!212.113.10.22!type!8!session_id!7318!reason!Close!start_time!2015-01-08 13:10:49!_ipAddress!10.181.100.87!_timestamp!1399601696!".split("!");
//    static String[] kvs = "line!18!device_id!WEST_STREET!duration!59!".split("!");

    /**
     * Seems to be the fastest
     * @param logId
     * @param lineNo
     * @param timeMs
     * @return
     */

    public Map<String,String> getAsMap(int logId, int lineNo, long timeMs){
        return getAsMapSingle(logId, lineNo, timeMs);
    }

    SystemIndexer systemIndexer = new SystemIndexer();
    int errorCount = 0;

    int lastLineNo = -1;
    String lastLineContent = null;
    boolean IS_REVERSING = System.getProperty("allow.reverse.break", "true").equals("true");
    final boolean DEBUG_ENABLED = LOGGER.isDebugEnabled();
    public Map<String,String> getAsMapSingle(int logId, int lineNo, long timeMs){



        if (DEBUG_ENABLED) LOGGER.debug(">> GetAsMap:" + logId + " Line:" + lineNo);
        if (raf == null || inLogId != logId || lastLineNo > lineNo){
            if (raf != null) {
                flushAndReleaseResources();
            }
            try {

                if (lineNo > 100 && lastLineNo > lineNo) {
                    //LOGGER.warn("LastLine:" + lastLineNo + " > " + lineNo + " Check LogId:" + logId + " NewLine Rule and ReIndex");


                    if (IS_REVERSING) {
//                        throw new RuntimeException("LastLine:" + lastLineNo + "\n\t DATA:" + lastLineContent + "\n\t > " + " Check LogId:" + logId + " NewLine Rule and ReIndex");
                        return new HashMap<>();

                    }
                }
                if (DEBUG_ENABLED) LOGGER.debug("Creating RAF:" + logId);
                File file = getFile(logId);
                if (!file.exists()) return nothing;
                raf = RafFactory.getRafSingleLine(file.getAbsolutePath());
                lastLineNo = 0;
                inLogId = logId;
            } catch (IOException e) {
                if (errorCount++ < 1000) e.printStackTrace();
                return nothing;
            }
        }

        HashMap<String, String> collecting = null;
        try {

            String rafLine = null;
            if (lastLineNo == lineNo && lastLineContent != null) {
                rafLine = lastLineContent;
                if (DEBUG_ENABLED) LOGGER.debug("ReRead Existing read -  logId:" + logId + " line:" + lineNo + " Content:" + lastLineContent.substring(0, 20));
            } else if (lastLineNo > lineNo) {
                // skip
                if (DEBUG_ENABLED) LOGGER.debug("Already exceeded line, logId:" + logId + " line:" + lineNo + " Waiting at:" + lastLineNo);
                return nothing;
            } else {
                rafLine = scrollToLine(raf, logId, lineNo);
            }
            if (rafLine == null) {
                if (DEBUG_ENABLED) LOGGER.debug("Missing Line for: LogId:" + logId + " line:" + lineNo);
                return nothing;
            }
            String leading = "_line!";
            String searchLine = lineNo + "!";

            if (!rafLine.startsWith(searchLine, leading.length())) {
                if (rafLine.startsWith("_line!")) {
                    lastLineContent = rafLine;
                    lastLineNo = Integer.parseInt(StringUtil.splitFastSCAN(rafLine, '!', 2));
                }

                if (errorCount++ < 1000) {
                    LOGGER.warn("LogId:" + logId +  " LookingFor:" + lineNo + " OutOfSyncIndex-GOT:" + rafLine );
                }
                return nothing;
            } else {
                lastLineNo = lineNo;
                lastLineContent = rafLine;
            }
            // ELSE - All good - proceed!
            String[] strings = StringUtil.splitFast2(rafLine, Pair.DELIM);

            collecting = new HashMap<>(strings.length/2);
            for (int i = 0; i < strings.length-1; i+=2) {
                collecting.put(strings[i], strings[i+1]);
            }

            systemIndexer.indexA(timeMs, collecting);

        } catch (IOException e) {
            if (errorCount++ < 1000) {
                e.printStackTrace();
            }

        }
        return collecting;
    }

    public String scrollToLine(RAF raf, int logId, int lineNo) throws IOException {

        boolean found = false;
        String leading = "_line!";
        String searchLine = lineNo + "!";
        String rafLine = raf.readLine();
        if (rafLine != null) found = rafLine.startsWith(searchLine, leading.length());

        if (!found) {
            if (LOGGER.isDebugEnabled()) {
                int len = rafLine.length() < 20 ? rafLine.length() : 20;
                LOGGER.debug("ScrollToLine LogId:" + logId + ":"  + lastLineNo + " seek:" +lineNo + " FOUND: " + rafLine.substring(0, len));
            }

            int scanned = 0;
            int startLine = -1;
            while (!found && scanned++ < lineNo) {
                // not ideal, we need to capture each line for mline events
                rafLine = raf.readLine();
                if (rafLine == null) found = true;
                else found = rafLine.startsWith(searchLine, leading.length());
                if (!found && rafLine != null) {
                    try {
                        int currentLine = Integer.parseInt(StringUtil.splitFastSCAN(rafLine, '!', 2));
                        if (currentLine > lineNo) {
                            LOGGER.warn("Got Line too High id:" + logId + " cline:" + currentLine + " Wanting:" + lineNo + "\n\t RAF:" + raf.getFilename() + " line:" + rafLine);
                            lastLineContent = rafLine;
                            lastLineNo = Integer.parseInt(StringUtil.splitFastSCAN(rafLine, '!', 2));
                            return null;
                        }
                        if (startLine == -1) startLine = currentLine;
                    } catch (Exception e) {
                        LOGGER.warn("FFIndexException Failed to parse ff.idx:" + logId + " E:" + e.toString() + " line:" + lineNo + " data:" + rafLine);
                    }
                }
            }
            //if (LOGGER.isDebugEnabled()) LOGGER.debug("LogId:" + logId + " Scrolled from:" + startLine + " Seeking:" + lineNo);
            if (scanned > lineNo) {
                LOGGER.warn("Overscan file:" + raf.getFilename() + " id:" + logId + " Failed to find line:" + searchLine);
                return null;
            }
        }


        if (LOGGER.isDebugEnabled()) {
            int len = rafLine.length() < 20 ? rafLine.length() : 20;
            LOGGER.debug("ScrollT/oLine LogId:" + logId + ": FOUND:" + found  + " WANTED:" + searchLine +  " GOTLastLine:" + rafLine.substring(0, len));
        }
        if (rafLine == null) throw new RuntimeException("Overscan file" + raf.getFilename() + "/" + logId + " Scrolled passed the end-of-index: logId:" + logId + " line:" + searchLine);
        return rafLine;
    }


    public List<Pair> get(int logId, int lineNo){
        return null;

    }

    public void remove(int logId){
        flushAndReleaseResources();
        getFile(logId).delete();

    }
    public void commit(){
        flushAndReleaseResources();
    }
    public void compact(){

    }
    public void close(){
        flushAndReleaseResources();

    }

    private void flushAndReleaseResources() {
        try {
            if (os != null) {
                os.close();
                os = null;
            }
        } catch (IOException e) {
            LOGGER.warn(e);
        }
        try {
            if (raf != null) {
                raf.close();
                raf = null;
            }
        } catch (IOException e) {
            LOGGER.warn(e);
        }
    }

    public boolean isRebuildRequired(){
        return false;
    }

    public void setKvExtractor(KVExtractor kve){
        this.kvExtractor = kve;
    }

    public void reset() {
        close();
    }
}
