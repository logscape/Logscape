package com.logscape.disco.indexer;

public interface DbOp {
    void remove(int logId);

    void commit();

    void close();
}
