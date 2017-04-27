package com.logscape.disco.kv;

import com.logscape.disco.indexer.Pair;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 25/04/2013
 * Time: 09:32
 * To change this template use File | Settings | File Templates.
 */
public class FileFavorite implements LineHandler {
    static final Logger LOGGER = Logger.getLogger(AllSlidersHandler.class);

    KeySlider[] sliders = null;

    public FileFavorite(KeySlider[] sliders) {
        this.sliders = sliders;
    }

    /**
     * Favourite is used to only process a single KV - which is the first to have found a hit in the file.
     * This is purely for scan performance as each Slider impacts performance - i.e. we only support 1 format per file
     */
    KeySlider favouriteSlider = null;

    /**
     * SinglePass Pre-Array-Entry-Tree
     */

    @Override
    public List<Pair> getFields(String line) {

        if (favouriteSlider != null) {
            return handleFavouriteSlider(line);
        }
        List<Pair> results = new ArrayList<Pair>();
        int length = line.length();
        for (int i = 0; i < length; i++) {
            if (favouriteSlider == null) {
                for (int j = 0; j < sliders.length; j++) {
                    if (sliders[j].next(line.charAt(i), i, length)) {
                        favouriteSlider = sliders[j];
                        break;
                    }
                }
            } else {
                favouriteSlider.next(line.charAt(i), i, length);
            }
        }


        if (favouriteSlider != null) {
            List<RulesKeyValueExtractor.Pair> results1 = favouriteSlider.results;
            for (RulesKeyValueExtractor.Pair pair : results1) {
                try {
                    String[] keyValue = pair.getKeyValue(line);
//                    if (!FieldSet.isDefaultField(keyValue[0])) {
                    if (!keyValue[0].startsWith("_")) {
                        results.add(new Pair(keyValue[0], keyValue[1]));
                    }
                } catch (Throwable t) {
                    LOGGER.warn("KVExtractFailed:" + pair + " KV line:\n\t" + line);
                }
            }
            favouriteSlider.reset();
        }

        return results;
    }

    @Override
    public List<RulesKeyValueExtractor.SliderRule> getRules() {
        return null;
    }
    public int getStartPos(){
        return 0;
    }



    private List<Pair> handleFavouriteSlider(String line) {
        List<Pair> results = new ArrayList<Pair>();
        int length = line.length();
        for (int i = 0; i < length; i++) {
            favouriteSlider.next(line.charAt(i), i, length);
        }
        List<RulesKeyValueExtractor.Pair> results1 = favouriteSlider.results();
        for (RulesKeyValueExtractor.Pair pair : results1) {
            try {
                String[] keyValue = pair.getKeyValue(line);
                results.add(new Pair(keyValue[0], keyValue[1]));
            } catch (Throwable t) {
                LOGGER.warn("KVExtractFailed:" + pair + " KV line:\n\t" + line);
            }
        }
        favouriteSlider.reset();
        return results;
    }
}
