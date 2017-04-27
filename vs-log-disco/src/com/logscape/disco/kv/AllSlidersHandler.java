package com.logscape.disco.kv;

import com.liquidlabs.common.StringUtil;
import com.logscape.disco.indexer.Pair;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 25/04/2013
 * Time: 09:32
 */
public class AllSlidersHandler implements LineHandler {
    static final Logger LOGGER = Logger.getLogger(AllSlidersHandler.class);

    boolean learn = false;
    private int startPos;
    KeySlider[] sliders = null;
    Map<String,RulesKeyValueExtractor.SliderRule> matched = new HashMap<String,RulesKeyValueExtractor.SliderRule>();
    int foundStartPos =  Integer.MAX_VALUE;

    public AllSlidersHandler(KeySlider[] sliders, boolean learn, int startPos) {
        this.sliders = sliders;
        this.learn = learn;
        this.startPos = startPos;
    }

    public AllSlidersHandler() {
    }

    @Override
    public List<RulesKeyValueExtractor.SliderRule> getRules() {
        return new ArrayList<RulesKeyValueExtractor.SliderRule>(matched.values());
    }
    public int getStartPos(){
        return this.foundStartPos;
    }

    /**
     * SinglePass Pre-Array-Entry-Tree
     */
    @Override
    public List<Pair> getFields(String line) {
        List<Pair> results = new ArrayList<Pair>();
        int length = line.length();
        for (int i = startPos; i < length; i++) {
            char charr = line.charAt(i);
            for (int j = 0; j < sliders.length; j++) {
                KeySlider slider = sliders[j];
                if (learn) {
                    if (slider.next(charr, i, length)) updateLearnRules(i, slider);
                } else {
                    if (slider.config.isWithin(i)) {
                        slider.next(charr, i, length);
                    }
                }
            }
        }
        TIntHashSet names = new TIntHashSet();

        boolean isStructured = isJson(line) || isXML(line);
        Set<String> collectedNames = new HashSet<String>();
        for (int j = 0; j < sliders.length; j++) {
            KeySlider slider = sliders[j];
            RulesKeyValueExtractor.SliderRule sliderRule = learn ?  matched.get(slider.getRule()) : null;

            List<RulesKeyValueExtractor.Pair> results1 = slider.results;
            for (RulesKeyValueExtractor.Pair pair : results1) {
                try {
                    if (learn) {
                        if (pair.from < foundStartPos) foundStartPos = pair.from;
                        sliderRule.update(pair.from);
                        sliderRule.update(pair.to);
                    }
                    String[] keyValue = pair.getKeyValue(line);


                    String fieldName = keyValue[0];
                    if (!isStructured && isNumber(fieldName)) continue;

                    int hashed = fieldName.hashCode();
                    if (names.contains(hashed)) {
                        if(collectedNames.contains(fieldName)) continue;
                    }
                    names.add(hashed);
                    collectedNames.add(fieldName);
                    //System.out.println("KeyValue:" + keyValue[0] + " - " +  keyValue[1]);
//                    if (!FieldSet.isDefaultField(fieldName)) {
                    if (isStructured || !fieldName.startsWith("_")) {
                        results.add(new Pair(fieldName,keyValue[1]));
                    }

                } catch (Throwable t) {
                    LOGGER.warn("KVExtractFailed:" + pair + " KV line:\n\t" + line);
                }
            }
            slider.reset();
        }
        return results;
    }

    private boolean isXML(String line) {
        return line.contains("<") && ( line.contains("<\\") || line.contains("//>") );
    }

    private boolean isJson(String line) {
        return line.contains("{") && line.contains("}");
    }

    boolean isNumber(String fieldName) {

        try {

            boolean b = StringUtil.isDouble(fieldName) != null;
            if (b) return true;

            boolean isNumericLength = fieldName.length() <= 2 || fieldName.length() == 4 || fieldName.length() == 6 || fieldName.length() == 8;
            if (!isNumericLength) return false;

            boolean isnonhexdig = fieldName.charAt(0) > 'f' &&  fieldName.charAt(0) <= 'z' || fieldName.charAt(0) > 'F' &&  fieldName.charAt(0) <= 'Z';
            if (isnonhexdig) return false;

            isnonhexdig = fieldName.charAt(1) > 'f' &&  fieldName.charAt(1) <= 'z' || fieldName.charAt(1) > 'F' &&  fieldName.charAt(1) <= 'Z';
            if (isnonhexdig) return false;

            long l = Long.parseLong(fieldName, 16);
            return true;
        } catch (Throwable t) {
            return false;
        }

    }

    private void updateLearnRules(int i, KeySlider slider) {
        int started = slider.started;
        String rule = slider.getRule();
        RulesKeyValueExtractor.SliderRule sliderRule = matched.get(rule);
        if (sliderRule != null) {
            sliderRule.update(i);
            sliderRule.update(started);
        } else {
            sliderRule = new RulesKeyValueExtractor.SliderRule(rule);
            sliderRule.update(i);
            sliderRule.update(started);
            matched.put(rule,sliderRule);
        }
        sliderRule.increment();
    }

}
