package com.liquidlabs.log.space.agg;

import com.liquidlabs.log.alert.report.*;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.space.LogRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 14/10/2015
 * Time: 13:48
 * To change this template use File | Settings | File Templates.
 */
public interface HistoAggListener {
    String notify(String subscriber, List<ClientHistoItem> list);

    void handleHisto(HistogramHandler histogramHandler, String providerId, String subscriber, List<Map<String, Bucket>> histo, LogRequest request);

    List<Map<String,Bucket>> getHisto(String subscriber);

    List<ClientHistoItem> getClientHistoFromRawHisto(LogRequest request, List<Map<String,Bucket>> histogram, HashMap<Integer, Set<String>> functionTags, List<FieldSet> fieldSets);

    List<Bucket> getHistoBuckets(String source);
}
