package com.liquidlabs.log.reader;

import com.logscape.disco.indexer.Pair;

import java.util.Collection;
import java.util.List;

/**
 * Created by neil.avery on 11/02/2016.
 */
public interface LineExtractor {
    boolean applies(String nextLine);

    List<Pair> extract(String nextLine);
}
