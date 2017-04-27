package com.liquidlabs.log.indexer.txn.krati;

import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.liquidlabs.log.indexer.krati.KratiFileStore;
import com.liquidlabs.transport.serialization.Convertor;
import krati.store.DataStore;
import krati.util.IndexedIterator;
import org.apache.log4j.Logger;

import java.util.*;

public class ListLogFilesCB {


	private static final Logger LOGGER = Logger.getLogger(ListLogFilesCB.class);
	private static final int MaxLimit = LogProperties.getMaxFileList();

	private DataStore<byte[], byte[]> store;
	private List<LogFile> results = new ArrayList<LogFile>();
	private long startTimeMs;
	private long endTimeMs;

	private LogFileOps.FilterCallback callback;

	public ListLogFilesCB(DataStore<byte[], byte[]> store, long startTimeMs, long endTimeMs, boolean sortByTime, LogFileOps.FilterCallback callback) {
		this.store = store;
		this.startTimeMs = startTimeMs;
		this.endTimeMs = endTimeMs;
		this.callback = callback;

	}
	public void doWork() {
			getAll();
	}

	private void getAll()  {

            IndexedIterator<byte[]> indexedIterator = store.keyIterator();
            while (indexedIterator.hasNext()) {
                byte[] key = indexedIterator.next();
                String filename = new String(key);
                if (filename.equalsIgnoreCase(KratiFileStore.FILE_SEED)) continue;

                try {

                    LogFile logFile = (LogFile) Convertor.deserialize(store.get(key));
                    if (startTimeMs > 0) {
                        if (logFile.isWithinTime(this.startTimeMs, this.endTimeMs)) {
                            if (callback.accept(logFile)) {
                                results.add(logFile);
                            }
                        }
                    } else {
                        if (callback.accept(logFile)) {
                            results.add(logFile);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Key" + filename, e);
                }
            }
	}
	
	public List<LogFile> getResults() {
		return results;
	}
	public void setFilter(LogFileOps.FilterCallback callback2) {
		this.callback = callback2;
	}
}
