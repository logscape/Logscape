package com.logscape.disco.kv;

import com.logscape.disco.indexer.Pair;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 25/04/2013
 * Time: 09:32
 * To change this template use File | Settings | File Templates.
 */
public class LineFavorite implements LineHandler {
    static final Logger LOGGER = Logger.getLogger(LineFavorite.class);

    int startPos = 0;
    int foundStartPos = Integer.MAX_VALUE;

    KeySlider[] sliders = null;
    private boolean learn;
    Map<String,RulesKeyValueExtractor.SliderRule> matched = new HashMap<String,RulesKeyValueExtractor.SliderRule>();

    public LineFavorite(KeySlider[] sliders, boolean learn, int startPos) {
        this.sliders = sliders;
        this.learn = learn;
        this.startPos = startPos;
    }

    List<Pair> noResults = new ArrayList<Pair>();
    /**
     * SinglePass Pre-Array-Entry-Tree
     */
    @Override
    final public List<Pair> getFields(final String line) {

        if (this.startPos == Integer.MAX_VALUE || sliders.length == 0) return noResults;

        KeySlider found = null;

        if (this.sliders.length == 1) {
            found = this.sliders[0];
        }

        List<Pair> results = new ArrayList<Pair>();

        int length = line.length();
        for (int i = startPos; i < length ; i++) {
            char c = line.charAt(i);
            if (found == null) {
                for (int j = 0; j < sliders.length; j++) {
                    KeySlider slider = sliders[j];
                    if (slider.config.isWithin(i)) {
                        if (slider.next(c, i, length)) {
                            found = slider;
                            if (learn) {
                                updateLearnRules(i, slider);
                            }
                            break;
                        }
                    }
                }
            } else {
                if (found.config.isWithin(i)) found.next(c, i, length);
            }
        }
        if (found != null) {
            extractValues(line, found.results, results);
            if (sliders[0] != found) {
                swapPositions(found);
            }
        }
        for (KeySlider slider : sliders) {
            slider.reset();
        }

        return results;
    }

    transient List<String> lastRules = null;
    @Override
    public List<RulesKeyValueExtractor.SliderRule> getRules() {
        return new ArrayList<RulesKeyValueExtractor.SliderRule>(matched.values());
    }
    public int getStartPos(){
        return this.foundStartPos;
    }

    final private void swapPositions(KeySlider forZero) {
        int foundPos = -1;
        for (int i = 0; i < sliders.length; i++) {
            if (sliders[i] == forZero) foundPos = i;
        }
        KeySlider currentZero = sliders[0];
        sliders[foundPos] = currentZero;
        sliders[0] = forZero;


    }
    final private void extractValues(final String line, List<RulesKeyValueExtractor.Pair> results1, List<Pair> results) {
        for (RulesKeyValueExtractor.Pair pair : results1) {
            try {
                if (learn && pair.from < foundStartPos) foundStartPos = pair.from;
                String[] keyValue = pair.getKeyValue(line);
//                if (!FieldSet.isDefaultField(keyValue[0])) {
                if (!keyValue[0].startsWith("_")) {
                    results.add(new Pair(keyValue[0],keyValue[1]));
                }

            } catch (Throwable t) {
                LOGGER.warn("KVExtractFailed:" + pair + " KV line:\n\t" + line);
            }
        }
    }
    private void updateLearnRules(int i, KeySlider slider) {
        String rule = slider.getRule();
        RulesKeyValueExtractor.SliderRule sliderRule = matched.get(rule);
        if (sliderRule != null) {
            sliderRule.update(i);
        } else {
            sliderRule = new RulesKeyValueExtractor.SliderRule(rule);
            sliderRule.update(i);
            matched.put(rule,sliderRule);
        }
    }

}