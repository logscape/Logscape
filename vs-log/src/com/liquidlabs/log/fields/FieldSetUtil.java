package com.liquidlabs.log.fields;

import com.liquidlabs.common.ExceptionUtil;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import com.liquidlabs.log.fields.field.FieldI;
import com.liquidlabs.log.fields.field.GroovyField;
import com.logscape.disco.kv.RulesKeyValueExtractor;
import org.apache.commons.lang3.StringEscapeUtils;

import java.io.File;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 09:36
 * To change this template use File | Settings | File Templates.
 */
public class FieldSetUtil {
    private static final String TESTING = "<b>Testing:</b>\t\t";
    private static final String SUCCESS = "<b>Success:</b>";
    private static final String SUCCESS_0 = SUCCESS + "0";
    private static final String EOL = "\n";
    private static final String GROUP_COUNT = "<b>Groups:</b>\t\t";
    private static final String STR = ") ";
    private static final String SPACE = " ";
    private static final String FUNC_TAG = " f:";
    private static final String TAB = "\t";



    static public boolean validate(FieldSet fieldSet, String fullFilePath, List<String> lines, String tag) {

        File file = new File(fullFilePath);
        String path = new File(FileUtil.getPath(file)).getParentFile().getAbsolutePath();
        if (!fieldSet.matchesFilenameAndPath(fullFilePath) && !fieldSet.matchesFilenameAndTAG(fullFilePath,tag)) return false;

        String[] linesArray = com.liquidlabs.common.collection.Arrays.toStringArray(lines);
        String testResult = test(fieldSet,linesArray);
        boolean passed = testResult.indexOf(SUCCESS_0) == -1 && testResult.indexOf(SUCCESS) > -1;
        return passed;
    }


    static public String test(FieldSet fieldSet, String[] lines) {
        StringBuilder result = new StringBuilder();
        String lastLine = "";
        int line = 1;
        int successCount = 0;
        int failureCount = 0;
        boolean fatalError = false;
        boolean loggedGroupCount = false;
        try {

            result.append(TESTING).append(fieldSet.id).append(EOL);
            result.append("<b>Expression:\t</b>").append(SimpleQueryConvertor.convertSimpleToRegExp(fieldSet.expression)).append(EOL);
            int lineNum = 0;
            fieldSet.addDefaultFields(fieldSet.id, "host.domain.com", "file.log", "/var/log/system.log", "tag", "Agent", "", 0, false);
            fieldSet.setIsDiscoveryEnabled(false);

            long start = 0;
            for (String nextLine : lines) {
                if (fatalError) continue;
                // only escape the first line
                if (lineNum++ == 0 && nextLine.startsWith("#")) continue;
                if (nextLine.length() == 0) continue;
                lastLine = nextLine;

                start = System.currentTimeMillis();
                String[] literalFieldValues = fieldSet.getFields(nextLine, 0, lineNum, System.currentTimeMillis());

                if (!loggedGroupCount) {
                    result.append(GROUP_COUNT + literalFieldValues.length).append(EOL).append(EOL);
                    loggedGroupCount = true;
                }
                nextLine = "<font color='#0000FF' size='12'>" + StringEscapeUtils.escapeXml(nextLine) + "</font>";
                result.append("<b>Data:</b>\t").append(nextLine).append(EOL);
                if (literalFieldValues.length == 0) {
                    fieldSet.getFields(nextLine, 0, lineNum, System.currentTimeMillis());
                    result.append(line++).append(STR).append("<b> <font color='#FF0000'>_FAILED_ to  expression-match line</font></b>:").append(nextLine).append(EOL).append(EOL);
                    failureCount++;
                    continue;
                }
                successCount++;
                int field = 0;
                Set<String> fieldNames = new HashSet<String>();

                for (FieldI afield : fieldSet.fields()) {
                    try {
                        if (fieldNames.contains(afield.name())) {
                            throw new RuntimeException("<b>Duplicate fields not supported ***_DUPLICATE_***=></b>" + fieldSet.id + "." + afield.name());
                        }
                        else fieldNames.add(afield.name());

                        start = System.currentTimeMillis();
                        String fieldExtracted = afield.get(literalFieldValues, new HashSet<String>(), fieldSet);

                        result.append("<b>").append(line).append(".").append(field + 1).append("</b>").append(STR).append(TAB).append(afield.name()).append("\t= ");
                        if (fieldExtracted == null)
                            fieldExtracted = "<b>***_FAILED_***</b>";

                        result.append(fieldExtracted);

                    } catch (Throwable t) {
                        failureCount++;
                        result.append(FUNC_TAG).append("<font color='#FF0000'>ERROR</font>");
                        fatalError = true;
                    }
//					result.append(SPACE).append("_PASS_");
                    result.append(EOL);
                    field++;
                }

                List<String> allErrors = fieldSet.allErrors();
                for (String msg : allErrors) {
                    result.append("<font color='#FF0000'>ERROR:</font> " + msg + EOL);
                }
                result.append("---------------------------------");
                result.append(EOL).append(EOL);


                line++;
            }

            return "<font size='12'><b>Field-Test Results</b></font>" + EOL + EOL + SUCCESS + successCount + " <b>Fail</b>:"+ failureCount + EOL + EOL +result;
        } catch (Throwable t) {
            t.printStackTrace();
            String exString = ExceptionUtil.stringFromStack(t, 2048);
            return "_FATAL_FAILED_ Line: " + line + ") " + lastLine + "\nException:" + exString + result.toString();

        } finally {
            // System.out.println(result.toString());
        }
    }

    /**
     * Benchmark 1 second
     * @return
     */
    public static String testPerformance(FieldSet fieldSet, String[] lines) {
        int lineNum = 0;
        fieldSet.addDefaultFields(fieldSet.id, "host.domain.com", "file.log", "/var/log/system.log", "tag", "Agent", "", 0, false);

        // heat up the cache
        String nextLinea = lines[0];
        if (nextLinea.startsWith("#") && lines.length > 2) nextLinea = lines[1];
        Map<String, AtomicLong> times = new LinkedHashMap<String,AtomicLong>();
        RulesKeyValueExtractor kve = new RulesKeyValueExtractor();
        fieldSet.setDiscovered(kve.getFields(nextLinea));

        String errors = scanFields(fieldSet, nextLinea, times);

        long start = System.currentTimeMillis();
        long end = System.currentTimeMillis();
        while ((end - start) < 2000) {
            int lpos = 0;
            for (String nextLine : lines) {
                if (lpos++ == 0 && nextLine.startsWith("#")) continue;
                scanFields(fieldSet, nextLine,times);
                lineNum++;
            }
            end = System.currentTimeMillis();
        }
        String benchmark = getBench(times, lineNum);
        long elapsed = (end - start);
        double lps = lineNum/(elapsed/1000.0);
        DecimalFormat format = new DecimalFormat("#,###.##");
        return "<b>Rate:</b>" + format.format(lps) + " linesPerSec\t <b>ElapsedMs:</b>" + elapsed + "\t <b>Lines:</b>" + format.format(lineNum) + "<br>" + benchmark + "<br> Errors:" + errors;
    }
    private static String getBench(Map<String, AtomicLong> times, int amount) {
        long fullScanElapsed = times.get("FullScan").get();
        StringBuilder result = new StringBuilder("<b>MicroBench</b> FullScan(ms):" + (fullScanElapsed/amount)/1000 + "  <br><b> Breakdown(%):</b><br>");
        for (String key : times.keySet()) {
            long totalNanos = times.get(key).get();
            result.append(key).append(":").append(Math.round((((double)totalNanos/(double)fullScanElapsed) * 100.0))).append("<br>");
        }
        return result.toString();
    }


    private static String scanFields(FieldSet fieldSet, String nextLine, Map<String, AtomicLong> times) {
        // need to allow for GroovyScipt code-cache-building


        long start = System.nanoTime();

        String[] events = fieldSet.getNormalFields(nextLine);
        update(times, "Expression Parse", start);

        StringBuilder errors = new StringBuilder();

        List<FieldI> fields2 = fieldSet.fields();
        for (FieldI field : fields2) {

            try {

                // ignore nonsummary fields
                if (FieldSet.isDefaultField(field.name())) continue;
                if (!field.isSummary()) continue;

                long start2 = System.nanoTime();
                field.get(events, new HashSet<String>(), fieldSet);

                String fieldname = field.name();
                fieldname += " [" + field.getClass().getSimpleName() + "] ";
                update(times, fieldname, start2);
            } catch (Throwable t) {
                update(times, field.name(), System.nanoTime());
                errors.append("Field:" + field.name() + " Exception:" + t.getMessage() + "<br>");

            }
        }
        update(times, "FullScan", start);
        return errors.toString();
    }
    static private void update(Map<String, AtomicLong> times, String key, long started) {
        if (!times.containsKey(key)) times.put(key, new AtomicLong());
        long delta = System.nanoTime() - started;
        times.get(key).addAndGet(delta);
    }

    public static String toXML(FieldSet fieldSet, String[] text) {
        StringBuilder results = new StringBuilder("<xml>");
        fieldSet.addDefaultFields(fieldSet.getId(),"host","file.log","/opt/directory/file.log","tag","Indexer","",1000,true);
        List<String> fieldNames = getAllFields(fieldSet, text);
        RulesKeyValueExtractor kve = new RulesKeyValueExtractor();


        int lineNum = 1;
        for (String line : text) {
            String[] events = fieldSet.getFields(line, -1, -1, System.currentTimeMillis());
            if (line.startsWith("#") || line.length() == 0) continue;

            fieldSet.setDiscovered(kve.getFields(line));

            results.append("<row>");
            for (String fieldName : fieldNames) {

                if (fieldSet.getField(fieldName) != null) {
                    fieldSet.getField(fieldName).setIndexed(false);
                }

                String fieldValue = fieldSet.getFieldValue(fieldName, events);
                if (fieldValue == null) fieldValue = "null";

                results.append("<" + StringUtil.fixForXML(fieldName) + ">");
                List<String> errors = fieldSet.allErrors();
                if (errors.size() > 0) {
                    fieldValue = "#ERROR " + errors;
                    errors.clear();
                }
                if (events.length == 0) {
                    fieldValue = "No Events = #ERROR";
                }
                results.append(StringEscapeUtils.escapeXml(fieldValue));
                results.append("</" + StringUtil.fixForXML(fieldName) + ">");
            }

            results.append("</row>\n");
        }
        results.append("</xml>");
        return results.toString();
    }

    private static List<String> getAllFields(FieldSet fieldSet, String[] text) {
        List<String> results = new ArrayList<String>();
        List<FieldI> fields = fieldSet.getFields();
        for (FieldI field : fields) {
            results.add(field.name());
        }

        for (String line : text) {
            fieldSet.getFields(line,-1,-1, System.currentTimeMillis());
            List<String> r2 = fieldSet.getFieldNames(true, true, true, false, true);
            for (String s : r2) {
                if (!results.contains(s)) results.add(s);
            }

        }
        return results;
    }
}
