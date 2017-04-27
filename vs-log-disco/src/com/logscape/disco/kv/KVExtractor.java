package com.logscape.disco.kv;

import com.logscape.disco.DiscoProperties;
import com.logscape.disco.indexer.Pair;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 19/04/2013
 * Time: 11:00
 * To change this template use File | Settings | File Templates.
 */
public interface KVExtractor {

    static int maxFieldValueLength = DiscoProperties.getMaxKVDiscoveredFieldValueLength();
    List<Pair> getFields(String line);

    RulesKeyValueExtractor.Config getConfig();
}
