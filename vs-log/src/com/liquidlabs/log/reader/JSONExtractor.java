package com.liquidlabs.log.reader;

import com.liquidlabs.log.search.functions.Function;
import com.logscape.disco.indexer.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.*;

/**
 * Created by neil.avery on 11/02/2016.
 */
public class JSONExtractor implements LineExtractor {
    @Override
    public boolean applies(String nextLine) {
        return nextLine.startsWith("{");

    }

    @Override
    public List<Pair> extract(String nextLine) {
        ArrayList<Pair> results = new ArrayList<>();
        if (isJson(nextLine)) {
            String context = "";
            Object parse = JSONValue.parse(nextLine);
            handleNext(context, parse, results);

        }
        Collections.sort(results, new Comparator<Pair>() {
            @Override
            public int compare(Pair o1, Pair o2) {
                return o1.key.compareTo(o2.key);
            }
        });
        return results;
    }

    private void handleNext(String context, Object value, ArrayList<Pair> results) {
        if (value instanceof JSONObject) {
            handle(context, (JSONObject) value, results);
        } else if (value instanceof JSONArray) {
            handle(context, (JSONArray) value, results);
        } else if (value instanceof String) {
            results.add(new Pair(context, (String) value));
        } else if (value instanceof  Long) {
            results.add(new Pair(context, ((Long)value).toString()));
        } else if (value instanceof  Double) {
            results.add(new Pair(context, ((Double)value).toString()));
        } else if (value instanceof  Boolean) {
            results.add(new Pair(context, ((Boolean) value).toString()));
        }
    }

    private void handle(String context, JSONArray parse, ArrayList<Pair> results) {
        ListIterator listIterator = parse.listIterator();
        int pos = 0;
        while (listIterator.hasNext()) {
            Object next = listIterator.next();
            String head = context.endsWith("_") ? context : context + "_";
            handleNext(head + pos++ , next, results);
        }
        pos++;
    }

    private void handle(String context, JSONObject parse, ArrayList<Pair> results) {
        for (Object e : parse.entrySet()) {
            if (e instanceof Map.Entry) {
                Map.Entry ee = (Map.Entry) e;
                Object key = ee.getKey();
                Object value = ee.getValue();
                    String head = context.endsWith("_")  || context.length() == 0 ? context : context + "_";
                    handleNext(head + key, value, results);

            }
        }
    }

    public String dedupe(String key) {
        for (int i = 0; i < Integer.getInteger("json.arr.max",200); i++) {
            if (key.indexOf("_") > 0 && key.indexOf(Integer.toString(i)) != -1) {
                String searching = "_" + i + "_";
                key = key.replace(searching, "_[]_");
                // needs to be applied 2x because replace doesnt backtrack (i.e. abc_0_0_field
                key = key.replace(searching, "_[]_");
                String endsWidth = "_" + i;
                if (key.endsWith(endsWidth)) {
                    key = key.substring(0, key.lastIndexOf(endsWidth)) + "_[]";
                }
            }
        }
        return key;
    }

    public static boolean isJson(String line) {
        return line.startsWith("{") || line.startsWith("[");
    }
}
