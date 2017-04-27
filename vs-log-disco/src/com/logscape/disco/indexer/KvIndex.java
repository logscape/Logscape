package com.logscape.disco.indexer;

/**
 */
public interface KvIndex extends DbOp{

    public void index(int logFileId, int lineNumber, String[] normalizedData);

    int[] get(int logId, int lineNo);

}
