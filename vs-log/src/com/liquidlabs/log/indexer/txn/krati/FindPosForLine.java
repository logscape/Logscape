package com.liquidlabs.log.indexer.txn.krati;

import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.transport.serialization.Convertor;
import krati.store.DataStore;

import java.io.IOException;

public class FindPosForLine extends KratiTxnBase {
    private final DataStore<byte[], byte[]> store;

    public FindPosForLine(DataStore<byte[], byte[]> store) {
        this.store = store;
    }

    public long get(LogFile logFile, long lineNo) {
        Bucket bucket = null;

        for (long time = logFile.getStartTime(); time < logFile.getEndTime(); time += bucketWidth) {
            byte[] pk = getPk(logFile.getId(), time);
            try {
                bucket = (Bucket) Convertor.deserialize(store.get(pk));
            } catch (Exception e) {
                continue;
            }
            if (bucket == null) continue;

            int bucketFrom = bucket.firstLine();
            int bucketTo = bucket.lastLine();

            if (bucketTo >= lineNo) {
                RAF raf = null;
                try {
                    long scanLength = lineNo - bucketFrom;
                    raf = RafFactory.getRaf(logFile.getFileName(), logFile.getNewLineRule());
                    raf.seek(bucket.startingPosition());
                    for (long l = 0; l < scanLength; l++) {
                        raf.readLine();
                    }
                    return raf.getFilePointer();
                } catch (IOException e) {
                    return -1L;
                } finally {
                    try {
                        raf.close();
                    } catch (IOException e) {
                        //
                    }
                }
            }

        }
        return -1L;

    }
}
