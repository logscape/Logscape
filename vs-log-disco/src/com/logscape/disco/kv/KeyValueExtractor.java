package com.logscape.disco.kv;

import com.logscape.disco.indexer.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 28/03/2013
 * Time: 09:51
 *
 * Default Supported KVs
 *
 * Json:    "key": "value"
 *          "key": value,
 *          "key": value
 *
 * KV-Type as below
 *  service=?URLs?
 *
 *  NOTE: 'Q' is used to escape " for json values
 *
 */

public class KeyValueExtractor implements KVExtractor {

    JSONArray tree;
    HashMap<Character,JSONObject> configMap = new HashMap<Character, JSONObject>();
    int minKeyLength = 2;

    public KeyValueExtractor() {
        this(defaultConfig);
    }
    public KeyValueExtractor(String config) {
        config = config.replaceAll("Q","\\\\\"");
        JSONObject parsed = (JSONObject) JSONValue.parse(config);
        this.tree = (JSONArray) parsed.get("dfp");
        Iterator iterator = tree.iterator();
        while (iterator.hasNext()) {
            JSONObject node = (JSONObject) iterator.next();
            String token = (String) node.get("token");
            configMap.put(token.charAt(0), node);
        }

    }
    /**
     * SinglePass Pre-Array-Entry-Tree
     */
    public List<Pair> getFields(String line) {

        List<Pair> results = new ArrayList<Pair>();
        int pos = 0;
        while (pos < line.length()) {
            JSONObject item = configMap.get(line.charAt(pos));
            if (item != null) {
                KeyValueExtractor.KeyValue kv = nextKeyValue(new KeyValueExtractor.KeyValue(), pos, line, item);
                while (kv.isMatch()) {
                    if (kv.key.length() > minKeyLength) {
                        results.add(new Pair(kv.key,kv.value));
                    }
                    pos = kv.pos-1;
                    kv = nextKeyValueScan(new KeyValueExtractor.KeyValue(), pos, line, item);
                }
            }
            pos++;
        }
        return results;
    }

    @Override
    public RulesKeyValueExtractor.Config getConfig() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * 1 Pass per root tree
     */
    public List<Pair> getFieldsMPass(String line) {

        List<Pair> results = new ArrayList<Pair>();

        Set<Character> keys = configMap.keySet();
        for (Character key : keys) {
            JSONObject item = configMap.get(key);
            int pos = 0;
            KeyValue kv = nextKeyValueScan(new KeyValue(), pos, line, item);
            while (kv.isMatch()) {
                if (kv.key.length() >= minKeyLength) {
                    results.add(new Pair(kv.key,kv.value));
                }
                pos = kv.pos;
                kv = nextKeyValueScan(new KeyValueExtractor.KeyValue(), pos, line, item);
            }
        }
        return results;
    }



    public List<Pair> getFieldsIter(String line) {

        List<Pair> results = new ArrayList<Pair>();
        Iterator iterator = tree.iterator();
        while (iterator.hasNext()) {
            JSONObject item = (JSONObject) iterator.next();
            KeyValueExtractor.KeyValue kv = nextKeyValueScan(new KeyValueExtractor.KeyValue(), 0, line, item);
            if (kv.isMatch()) {
                while (kv.isMatch()) {
                    if (kv.key.length() >= 2) results.add(new Pair(kv.key,kv.value));
                    kv = nextKeyValueScan(new KeyValueExtractor.KeyValue(), kv.pos, line, item);
                }
            }
        }
        return results;
    }

    /**
     * Tree based stack thing (hopefully better at pruning the path)
     */
    public KeyValue nextKeyValueScan(KeyValue kv, int pos, String line, JSONObject node) {

        while (pos < line.length()) {
            kv = nextKeyValue(kv,pos,line,node);
            if (kv.isDone()) return kv;
            // any better?
            // can u hash the first character lookup???
            if (kv.matches.size() > 0) {
                pos = kv.matches.get(0).pos;
            }
            else {
                pos++;
            }
            kv.reset();
            //
            //this this wrong and makes it slow...
//            pos++;
        }

        return kv;
    }
    public KeyValue nextKeyValue(KeyValue kv, int pos, String line, JSONObject node) {

        String token = (String) node.get("token");
        String matchStyle = (String) node.get("match");
        Match match = findMatch(pos, line, token, matchStyle);

        pos = match.pos;
        if (match.isMatch) {
            pos = match.pos;
            kv.addMatch(match);

            if (kv.isDone()) return kv;
            JSONArray children = (JSONArray) node.get("children");
            if (children == null) {
                return kv;
            }
            Iterator iterator = children.iterator();
            while (iterator.hasNext()) {
                KeyValue keyValue = nextKeyValue(kv, pos, line, (JSONObject) iterator.next());
                if (keyValue.isDone()) {
                    keyValue.build(line);
                    return keyValue;
                }
            }

            // BRANCH FAILED so unroll the collected matches
            kv.reset();
        } else {
            return kv;
        }

        return kv;
    }
    /**
     * Iteration based brute force thing
     */

    public KeyValue nextKeyValue(int pos, String line, String[] from, String[] split, String[] to) {


        int loop = 0;
        while (pos < line.length()) {
            /**
             * key start
             */
            Match fm = findMultiMatch(pos, line, from, "none");
            pos = fm.pos;
            if (!fm.isMatch) continue;

            /**
             *  split
             */

            Match sm = findMultiMatch(pos, line, split, "alphaNumeric");
            pos = sm.pos;
            if (!sm.isMatch) continue;


            /**
             * value end
             */
            Match tm = findMultiMatch(pos, line, to, "none");
            pos = tm.pos;
            if (!tm.isMatch) continue;

            // We got a match
            return new KeyValue(line, fm,sm,tm);


        }
        return null;
    }


    Match findMultiMatch(int start, String line, String[] delimiter, String matchType) {

        if (delimiter.length == 1) return findMatch(start, line, delimiter[0], matchType);

        int loop = 0;
        Match match = null;
        while (loop < delimiter.length) {
            match = findMatch(start, line, delimiter[loop], matchType);
            if (match.isMatch) break;
            loop++;
        }
        return match;
    }
    Match findMatch(int start, String line, String delimiter, String matchType) {
        Match match = new Match();

        int mpos = 0;
        Restrictor restricter = Restrictor.get(matchType);

        // exclude numeric only keys when alphaNumeric?

        for (int i = start; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == delimiter.charAt(mpos)) {
                mpos++;
                if (mpos == delimiter.length()) {
                    return match.set(delimiter,true,i+1);
                }

            } else {
                if (restricter.isExcluded(c)) return match.set(delimiter,false,i);

                mpos = 0;
            }
        }

        return match.set(delimiter,false,line.length());
    }
    public static class Restrictor {
        char smallFrom = 'a';
        char smallTo = 'z';
        char bigFrom = 'A';
        char bigTo = 'Z';
        char numFrom = '0';
        char numTo = '9';
        static Restrictor alpha = new AlphaRestricter();
        static Restrictor alphaNumeric = new AlphaNumericRestricter();
        static Restrictor none = new NoneRestricter();
        public static Restrictor get(String type) {
            if (type == null) return none;
            if (type.equalsIgnoreCase("alphaNumeric")) return alphaNumeric;
            else if (type.equalsIgnoreCase("alpha")) return alpha;
            else return none;
        }

        public Restrictor(){}
        public boolean isExcluded(char c) {
            return true;
        }
        public static class NoneRestricter extends Restrictor {
            public boolean isExcluded(char c) { return false; }
        }
        public static class AlphaRestricter extends Restrictor {
            public boolean isExcluded(char c) {
                return  ! (c >= smallFrom && c <= smallTo ||
                        c >= bigFrom && c <= bigTo);
            }
        }
        public static class AlphaNumericRestricter extends Restrictor {
            public boolean isExcluded(char c) {
                return !(c >= smallFrom && c <= smallTo ||
                        c >= bigFrom && c <= bigTo ||
                        c >= numFrom && c <= numTo);
            }
        }
    }

    public static class KeyValue {
        String key;
        String value;
        int pos;

        public KeyValue(){}

        public KeyValue(String line, Match from, Match split, Match to) {
            build(line, from, split, to);
            this.pos = to.pos;
        }
        List<Match> matches = new ArrayList<Match>();
        public void addMatch(Match match) {
            if (match.isMatch) {
                matches.add(match);
                this.pos = match.pos;
            }
        }
        public void reset() {
            matches.clear();
        }
        public boolean isDone() {
            return matches.size() == 3;
        }

        public void build(String line) {
            if (key == null) build(line, matches.get(0),matches.get(1),matches.get(2));
        }
        public void build(String line, Match from, Match split, Match to) {
            this.key = line.substring(from.pos, split.pos - split.token.length());
            this.value = line.substring(split.pos,to.pos - to.token.length());
        }
        public String toString() {
            return "kv " + key + ":" + value + " pos:" + pos;
        }

        public boolean isMatch() {
            return matches.size() == 3;
        }
    }

    public static class Match {
        String token;
        boolean isMatch;
        int pos;

        public Match() {
        }

        public Match set(String token, boolean b, int i) {
            this.token = token;
            this.isMatch = b;
            this.pos = i;
            return this;
        }
    }
    static String defaultConfig =
            " { \"dfp\": [\n" +
                    "            {\n" +
                    "                \"name\": \"json\",\n" +
                    "                \"token\": \"Q\",\n" +
                    "                \"children\": [\n" +
                    "                        {\n" +
                    "                                \"token\": \"Q: Q\", \"match\": \"alphaNumeric\",  \"children\": [ {\"token\": \"Q\" } ]\n" +
                    "                        },\n" +
                    "                        {\n" +
                    "                            \"token\": \"Q: \",  \"match\": \"alphaNumeric\",      \"children\": [ {\"token\": \",\" },\n" +
                    "                                                                                    {\"token\": \" \" }]\n" +
                    "                        }]\n" +
                    "            },{\n" +
                    "                \"name\": \"kv\",\n" +
                    "                    \"token\": \" \",\n" +
                    "                    \"children\": [\n" +
                    "                        {\n" +
                    "                            \"token\": \"=Q\",  \"match\": \"alphaNumeric\", \"children\": [ {\"token\": \"Q\" } ]\n" +
                    "                        },\n" +
                    "                        {\n" +
                    "                            \"token\": \":\",   \"match\": \"alphaNumeric\", \"children\": [ {\"token\": \" \",\"match\": \"alphaNumeric\" } ]\n" +
                    "                        }\n" +
                    "                    ]\n" +
                    "            }\n" +
                    "            ] }";

}
