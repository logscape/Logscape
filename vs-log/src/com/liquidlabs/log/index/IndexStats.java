package com.liquidlabs.log.index;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.file.FileUtil;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.text.DecimalFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class IndexStats {
	private static final Logger SLOGGER = Logger.getLogger("LoggerStatsLogger");
	static DecimalFormat formatter = new DecimalFormat("#.##");

    Map<String, Stuff> map = new HashMap<String, Stuff>();

    public int totalFiles() {
        Collection<Stuff> values = map.values();
        int results = 0;
        for (Stuff value : values) {
            results += value.totalFiles;
        }

        return results;
    }

    public long indexedToday() {
        Collection<Stuff> values = map.values();
        long results = 0;
        for (Stuff value : values) {
            results += value.indexedToday;
        }

        return results;
    }

    public long indexedTotal() {
        Collection<Stuff> values = map.values();
        long results = 0;
        for (Stuff value : values) {
            results += value.indexedTotal;
        }
        return results;
    }

    public int totalTags() {
        Collection<Stuff> values = map.values();
        int results = 0;
        for (Stuff value : values) {
            results += value.tagItems.size();
        }
        return results;
    }

    public static class Stuff {
        public int totalFiles;
        public long indexedToday;
        public long indexedTotal;
        public Map<String, AtomicLong> tagData = new HashMap<String, AtomicLong>();
        public Map<String, AtomicLong> tagItems = new HashMap<String, AtomicLong>();
    }

	String msg;
	
	public void setMsg(String msg) {
		this.msg = msg;
	}
	public String getMsg() {
		return msg;
	}
	
	public String toString() {
		return new StringBuilder().append(" Tag:").append(map.toString()).append(" Data:").append(map.toString()).toString();
	}

	public void reset() {
	}
	public void log(IndexStats lastStats) {
        for (String host : map.keySet()) {
            Stuff stuff = map.get(host);
            Stuff lastStuff = lastStats.map.get(host);

            SLOGGER.info("LS_EVENT:STATS Host:" + host + "  Files:" + stuff.totalFiles);
            SLOGGER.info("LS_EVENT:DATA_GB Host:" + host + " Total:" + toGB(stuff.indexedTotal) + " Today:" + toGB(stuff.indexedToday) );

            if (lastStats == null || lastStuff == null) continue;

            for (String tag : stuff.tagData.keySet()) {
                AtomicLong data = stuff.tagData.get(tag);
                AtomicLong lastData = lastStuff.tagData.get(tag);
                double dataDelta = data.get();
                if (lastData != null) {
                    dataDelta -= lastData.get();
                }
                AtomicLong items = stuff.tagItems.get(tag);
                AtomicLong lastItems = lastStuff.tagItems.get(tag);
                long itemDelta = items.get();
                if (lastItems != null) {
                    itemDelta -= lastItems.get();
                }
                SLOGGER.info("LS_EVENT:TAG Host:" + host + " tag:" + tag + " _gb:" + formatter.format((double)dataDelta/FileUtil.GB) + " _items:" + itemDelta);
            }
        }
	}
	private String toGB(double value) {
		return formatter.format(FileUtil.getGIGABYTES(value));
	}
	public void update(LogFile logFile) {
		Stuff stuff = updateTotalStats(logFile);

		String tags = logFile.getTags();
		if (tags != null) {
			String[] allTags =tags.split(",");
			for (String tag : allTags) {
				if (!stuff.tagData.containsKey(tag)) stuff.tagData.put(tag, new AtomicLong());
				stuff.tagData.get(tag).addAndGet(logFile.getPos());
				if (!stuff.tagItems.containsKey(tag)) stuff.tagItems.put(tag, new AtomicLong());
				stuff.tagItems.get(tag).incrementAndGet();
			}
		}
	}	
	private Stuff updateTotalStats(LogFile logFile) {
        String fileHost = logFile.getFileHost(NetworkUtils.getHostname());
        Stuff stuff = map.get(fileHost);
        if (stuff == null) {
            stuff = new Stuff();
            map.put(fileHost, stuff);
        }

        stuff.totalFiles++;
		stuff.indexedTotal += logFile.getPos();
		
		if (isToday(new DateTime(logFile.getEndTime()))) {
			stuff.indexedToday += logFile.getPos();
		}
        return stuff;
	}
	private boolean isToday(DateTime dateTime) {
		return new DateTime().getDayOfYear() == dateTime.getDayOfYear();
	}

}
