package com.logscape.disco.indexer;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 05/01/15
 * Time: 19:31
 * To change this template use File | Settings | File Templates.
 */
public class Pair {
    public static final char DELIM = '!';
    public static final char REPL = '_';
    public String key;
    public String value;

    public Pair(String key, String value) {
        if (key.contains(" ")) key = key.replace(" ","_");
        this.key = key;
        this.value = value;
    }

    public void toFlatFile(StringBuilder sb) {
        if (key.indexOf(DELIM) != -1) key = key.replace(DELIM,REPL);
        if (value.indexOf(DELIM) != -1) value = value.replace(DELIM,REPL);
        if (value.indexOf("\n") > -1) value = value.substring(0, value.indexOf("\n"));

        if (value.length() > 0) {
            sb.append(key).append(DELIM).append(value.trim()).append(DELIM);
        }
    }

    @Override
    public String toString() {
        return "(Pair:" + key + ":" + value + ")";
    }
}
