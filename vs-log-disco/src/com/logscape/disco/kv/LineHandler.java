package com.logscape.disco.kv;

import com.logscape.disco.indexer.Pair;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 25/04/2013
 * Time: 09:31
 * To change this template use File | Settings | File Templates.
 */
public interface LineHandler {
    List<Pair> getFields(String line);

    List<RulesKeyValueExtractor.SliderRule> getRules();

    int getStartPos();
}
