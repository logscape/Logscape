package com.liquidlabs.log.indexer.txn.pi;

import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.BucketKey;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.indexer.persistit.PILineStore;
import com.logscape.disco.indexer.Db;

import java.io.IOException;

public class PIFindPosForLine extends PITxnBase {
    private Db<BucketKey, Bucket> store;
    private PILineStore lineStore;

    public PIFindPosForLine(Db<BucketKey, Bucket> store, PILineStore lineStore) {
        this.store = store;
        this.lineStore = lineStore;
    }
    long result = -1L;
    public long get(final LogFile logFile, final long lineNo) {



        lineStore.iterateBuckets(logFile.getId(), logFile.getStartTime(), logFile.getEndTime(), new PILineStore.Task(){
            @Override
            public boolean run(Bucket bucket) {
                result = bucket.startingPosition();

                if (bucket.containsLine((int) lineNo)) {

                    RAF raf = null;
                    try {
                        int realLine = bucket.firstLine;
                        long scanLength = lineNo - bucket.firstLine();
                        raf = RafFactory.getRaf(logFile.getFileName(), logFile.getNewLineRule());
                        raf.seek(bucket.startingPosition());
                        int linesRead = 100;
                        while (linesRead > 0 && realLine < lineNo) {
                            raf.readLine();
                            linesRead = raf.linesRead();
                            realLine+=linesRead;
                        }
                        result = raf.getFilePointer();
                        return true;
                    } catch (IOException e) {
                    } finally {
                        try {
                            raf.close();
                        } catch (IOException e) {
                            //
                        }
                    }
                }
                return false;
            }
        });


        return result;

    }
}
