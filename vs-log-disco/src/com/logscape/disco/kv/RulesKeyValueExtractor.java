package com.logscape.disco.kv;

import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * find valid keys from syntax
 *  service="WorkstationFiles",serverName="Gateway",feHost="cauthstagea02",
 * "\"(Aa1)\":"
 *
 */
public class RulesKeyValueExtractor implements KVExtractor {

    static final Logger LOGGER = Logger.getLogger(RulesKeyValueExtractor.class);

    private LineHandler lineHandler;

    public RulesKeyValueExtractor(String... config) {
        this(new Config(Arrays.asList(config), true));
    }

    public RulesKeyValueExtractor() {
        this(new Config(Arrays.asList(new String[] {
                                                        "[\t, ](A1.):'(^'\n)[']",
                                                        "[\t, ](A1.):(^,: \n)[\n\t, ]" ,
                                                        "[\t, ](A1.): (^,: \n)[\n\t, ]" ,
                                                        "[\"](A1:.)\": \"(^\"\n])[\"]",
                                                        "[\"](A1:.)\":\"(^\"])[\"]",
                                                        "[?&\t, \n|](A1.)=(A1.)[\n\t, &?]",
                                                        "[\t, [](A1.)='(^'\n)[']",
                                                        "[\t, [](A1.) = '(^'\n)[']",
                                                        "[\t, [](A1:.)=\"(^\"\n])[\"]",
                                                        "[\t, /[](A1.) = (^, =\n])[\n\t, ]",
                                                        "[<:](A1.)>(^<>)",
                                                        "[<](A1.)>(^<>)"
        }), true));
    }

    KeySlider[] sliders = null;
    public RulesKeyValueExtractor(Config config) {
        sliders = new KeySlider[config.rules.size()];
        for (int i = 0; i < config.rules.size(); i++) {
            try {
                sliders[i] = new KeySlider(config.rules.get(i));
            } catch (Throwable t) {
                t.printStackTrace();;
                LOGGER.warn("Ignoring RULE:" + config.rules.get(i) + " EX:" + t.toString());
            }
        }
//        lineHandler = new LineFavorite(sliders, config.learn, config.startPos);
        lineHandler = new AllSlidersHandler(sliders, config.learn, config.startPos);
    }
    public Config getConfig() {
        return new Config(lineHandler.getRules(), false, lineHandler.getStartPos());
    }


    @Override
    public List<com.logscape.disco.indexer.Pair> getFields(String line) {
        return lineHandler.getFields(line);
    }

    public static class Pair {
        int from;
        int to;
        int end;

        private String toToken;

        public Pair(int from, int to, int end, String toToken) {
            this.from = from;
            this.to = to;
            this.end = end;
            this.toToken = toToken;
        }
        public String toString() {
            return "Pair(" + from + "," + to + "," + end + ")";
        }
        public String[] getKeyValue(String line) {
            return new String[] { line.substring(from + 1, 1 + to - toToken.length()), line.substring(to + 1, end) };
        }
    }

    static Map<String,String> toTokensCache = new ConcurrentHashMap<String, String>();
    static public String getPostText(String keyFormat) {
        int endGroup1 = keyFormat.indexOf(")")+1;
        String result = keyFormat.substring(endGroup1, keyFormat.indexOf("(", endGroup1));
        if (result != null) {
            String maybe = toTokensCache.get(result);
            if (maybe != null) return maybe;
            toTokensCache.put(result, result);
        }

        return result;
    }

    static public KVRule.Rule getPreText(String keyFormat) {
        return CharMatcherRuleFactory.getMatcher(keyFormat.substring(0, keyFormat.indexOf("(")));
    }



    static public KVRule.Rule getKeyRule(String s) {
        String[] part = s.split("\\)");
        return KVRule.Rule.get(part[0] + ")");
    }
    static public KVRule.Rule getValueRule(String s) {
        int last = s.lastIndexOf("(");
        String got =    s.substring(last, s.lastIndexOf(")")+1);
        return KVRule.Rule.get(got);
    }
    static public KVRule.Rule getPostRule(String s) {
        int last = s.lastIndexOf(")[");
//        if (last == -1 && s.endsWith("]")) return null;
        if (last == -1) return null;
        String lastBit = s.substring(last+1, s.length()-1);
        return KVRule.Rule.get(lastBit);
    }

    public static Config DEFAULT_CONFIG = new Config();
    public static class Config {
        List<SliderRule> rules = new ArrayList<SliderRule>();
        public boolean learn;
        public int startPos = Integer.MAX_VALUE;

        public Config() {}
        public Config(List<String> rules, boolean learn) {
            for (String rule : rules) {
                this.rules.add(new SliderRule(rule));
            }
            this.learn = learn;
            if (learn) startPos = 0;
        }
        public Config(List<SliderRule> rules, boolean learn, int startPos) {
            this.rules = rules;
            this.learn = learn;
            this.startPos = startPos;
        }

        public void merge(Config kveConfig) {
            List<SliderRule> otherRules = kveConfig.rules;
            for (SliderRule s : otherRules) {
                int i = this.rules.indexOf(s);
                if (i == -1) {
                     this.rules.add(s);
                } else {
                    SliderRule myRule = this.rules.get(i);
                    myRule.merge(s);
                }
            }
            if (kveConfig.startPos < this.startPos) this.startPos = kveConfig.startPos;

        }
        public String toString() {
            return "KVConfig offset:" + this.startPos + " Rules:" + rules.toString();
        }
        public List<SliderRule> getRules(){
            return rules;
        }
    }
    public static class SliderRule {
        String rule;
        short from = Short.MAX_VALUE;
        short to = 0;
        public int hits;

        public SliderRule(String rule) {
            this.rule = rule;
        }
        public void update(int pos) {
            if (pos < from) from = (short) pos;
            if (pos > to) to = (short) pos;
        }
        public boolean isWithin(int pos) {
            return pos >= from  && pos <= to ;
        }

        public int hashCode() {
            return rule.hashCode();
        }

        public boolean equals(Object o) {
            return rule.equals(((SliderRule) o).rule);
        }

        public void merge(SliderRule s) {
            this.update(s.from);
            this.update(s.to);

            this.hits += s.hits;
            if (this.hits < 0) this.hits = Integer.MAX_VALUE;
        }

        public String toString() {
            return "KVR:" + rule + " " + from + "-" + to + " h:" + hits;
        }

        public void increment() {
            if (hits < Integer.MAX_VALUE) hits++;
        }
    }

}
