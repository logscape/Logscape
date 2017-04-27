package com.liquidlabs.log;

import com.liquidlabs.log.index.InMemoryIndexer;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.log.streaming.LiveHandler;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TailerImplTest {

	@Test
	public void noDataSecondsUnder1Hour() throws Exception {
		
		File file = new File("test-data/agent.log");
		InMemoryIndexer indexer = new InMemoryIndexer();
		indexer.open(file.getAbsolutePath(), true, "basic", "tag");
		TailerImpl tailer = new TailerImpl(file, 0, 0, new MyLogReader(), new WatchDirectory(), indexer);
		assertEquals(1, tailer.calculateRescheduleSeconds(10));
		assertEquals(1, tailer.calculateRescheduleSeconds(60));
		assertEquals(1, tailer.calculateRescheduleSeconds(90));
		assertEquals(1, tailer.calculateRescheduleSeconds(59 * 60));
		
		// 1 hour == 3 seconds
		assertEquals(3, tailer.calculateRescheduleSeconds(60 * 60));
		
		// 3 hour == 4 seconds
		assertEquals(7, tailer.calculateRescheduleSeconds( 3 * 60 * 60));

		// 24 hours == 30 seconds
		assertEquals(49, tailer.calculateRescheduleSeconds(24 * 60 * 60));
	}
	public static class MyLogReader implements LogReader {

		public void addLiveHandler(LiveHandler liveHandler) {
		}

		public void deleted(String filename) {
		}

		public Indexer getIndexer() {
			return null;
		}

		public long getLastUsedTime() {
			return 0;
		}

        public long getLastTimeExtracted() {
            return System.currentTimeMillis();
        }


        public long getTime(String filename, int lineNumber, String nextLine, long fileStartTime, long fileLastMod, long filePos, long fileLength) {
			return 0;
		}

		public void interrupt() {
		}

        @Override
        public void setLogFiletype(String fieldSetId) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

		@Override
		public int currentLine() {
			return 0;
		}

		public WriterResult readNext(long startPos, int currentLine)
				throws IOException {
			return null;
		}

		public void roll(String from, String to, long currentPos, int line) {
		}

        @Override
        public void roll(String from, String to) {
        }

        public void setFilters(String[] includes, String[] excludes) {
		}

		public void setLogId(LogFile logFile) {
		}

		public void stopTailing() {
		}
		
	}

}
