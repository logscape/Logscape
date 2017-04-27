package com.logscape.disco.indexer;

import com.liquidlabs.common.collection.CompactCharSequence;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 30/01/2014
 * Time: 10:43
 * To change this template use File | Settings | File Templates.
 */
public interface Dictionary {
    Db<Integer, String> get(int logId);

    int[] normalize(TupleIntInt kk, String[] normalizedData);

    void remove(int logId);

    void compact();

    void commit();

    void close();
}
