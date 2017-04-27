package com.logscape.disco.indexer;

import java.util.List;
import java.util.Map;

public interface LookUpTable extends DbOp {
    String[] normalize(int logId, List<Pair> fieldIs);

    List<Pair> get(int logId, int[] normalizedData, long timeMs);
    Map<String,String> getAsMap(int logId, int[] normalizedData, long timeMs);

}
