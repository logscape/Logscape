package com.liquidlabs.log.search.summaryindex;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogRequestBuilder;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.functions.Count;
import com.liquidlabs.log.search.functions.IntValue;
import com.liquidlabs.log.space.LogRequest;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.is;

/**
 * Created by neil.avery on 11/12/2015.
 */
public class PersistingSummaryIndexTest {

    @Test
    public void testShouldPreventRepeatedWriteBySameFileSource() throws Exception {

        PersistingSummaryIndex persistingSummaryIndex = new PersistingSummaryIndex("build/SIDX");
        LogRequest write = new LogRequestBuilder().getLogRequest("sub", Arrays.asList("* | _filename.count() _path.count() summary.index(write)"), "", new DateTime().minusHours(24).getMillis(), new DateTime().getMillis());
        LogRequest read = new LogRequestBuilder().getLogRequest("sub", Arrays.asList("* | _filename.count() _path.count() summary.index(delete)"), "", new DateTime().minusHours(24).getMillis(), new DateTime().getMillis());

        // check the write bucket Width is 10 minutes
        assertEquals(10 * DateUtil.MINUTE, write.getBucketWidthSecs() * 1000);

        // write a real bucket for persisting
        Bucket bucket = new Bucket(write.getStartTimeMs(), write.getStartTimeMs() + 1000, write.query(0).functions(), 0, "*", "source", "sub", "file1.log");

        String hostname = "host";
        String resource = "source";
        FieldSet fieldSet = FieldSets.getBasicFieldSet();
        fieldSet.addDefaultFields("log4j", hostname, "file.log", "/var/log/file.log", "log4j", "Agent", resource, 0, true);
        long time = System.currentTimeMillis();
        bucket.increment(fieldSet, new String[] {"line"}, "file.log", "/var/log/file.log", time, time, time, 100, "line", new MatchResult("file"), false, time, time);

        persistingSummaryIndex.write(write.cacheKey(), bucket);
        persistingSummaryIndex.write(write.cacheKey(), bucket);

        persistingSummaryIndex.flush(write.cacheKey());

        // read back through a different instance
        PersistingSummaryIndex readIndex = new PersistingSummaryIndex("build/SIDX");

        List<Bucket> buckets = readIndex.read(read.cacheKey(), 0, System.currentTimeMillis());
        assertThat(buckets.get(0).hits(), is(equalTo(1)));

    }


    @Test
    public void testDelete() throws Exception {

        PersistingSummaryIndex persistingSummaryIndex = new PersistingSummaryIndex("build/SIDX");
        LogRequest write = new LogRequestBuilder().getLogRequest("sub", Arrays.asList("* | _filename.count() _path.count() summary.index(write)"), "", new DateTime().minusHours(24).getMillis(), new DateTime().getMillis());
        LogRequest read = new LogRequestBuilder().getLogRequest("sub", Arrays.asList("* | _filename.count() _path.count() summary.index(delete)"), "", new DateTime().minusHours(24).getMillis(), new DateTime().getMillis());

        // check the write bucket Width is 10 minutes
        assertEquals(10 * DateUtil.MINUTE, write.getBucketWidthSecs() * 1000);

        // write a real bucket for persisting
        Bucket bucket = new Bucket(write.getStartTimeMs(), write.getStartTimeMs() + 1000, write.query(0).functions(), 0, "*", "source", "sub", "file1.log");

        String hostname = "host";
        String resource = "source";
        FieldSet fieldSet = FieldSets.getBasicFieldSet();
        fieldSet.addDefaultFields("log4j", hostname, "file.log", "/var/log/file.log", "log4j", "Agent", resource, 0, true);
        long time = System.currentTimeMillis();
        bucket.increment(fieldSet, new String[] {"line"}, "file.log", "/var/log/file.log", time, time, time, 100, "line", new MatchResult("file"), false, time, time);

        persistingSummaryIndex.write(write.cacheKey(), bucket);
        persistingSummaryIndex.flush(write.cacheKey());

        // read back through a different instance
        PersistingSummaryIndex readIndex = new PersistingSummaryIndex("build/SIDX");
        String dir = readIndex.delete(read.cacheKey(), null, "host", read);

        assertFalse(new File(dir).exists());
    }
    @Test
    public void testWrite() throws Exception {

        PersistingSummaryIndex persistingSummaryIndex = new PersistingSummaryIndex("build/SIDX");
        LogRequest write = new LogRequestBuilder().getLogRequest("sub", Arrays.asList("* | _filename.count() summary.index(write)"), "", new DateTime().minusHours(24).getMillis(), new DateTime().getMillis());
        LogRequest read = new LogRequestBuilder().getLogRequest("sub", Arrays.asList("* | _filename.count() summary.index(read)"), "", new DateTime().minusHours(24).getMillis(), new DateTime().getMillis());

        // check the write bucket Width is 10 minutes
        assertEquals(10 * DateUtil.MINUTE, write.getBucketWidthSecs() * 1000);

        // write a real bucket for persisting
        Bucket bucket = new Bucket(write.getStartTimeMs(), write.getStartTimeMs() + 1000, write.query(0).functions(), 0, "*", "source", "sub", "");

        String hostname = "host";
        String resource = "source";
        FieldSet fieldSet = FieldSets.getBasicFieldSet();
        fieldSet.addDefaultFields("log4j", hostname, "file.log", "/var/log/file.log", "log4j", "Agent", resource, 0, true);
        long time = System.currentTimeMillis();
        bucket.increment(fieldSet, new String[] {"line"}, "file.log", "/var/log/file.log", time, time, time, 100, "line", new MatchResult("file"), false, time, time);

        persistingSummaryIndex.write(write.cacheKey(), bucket);
        persistingSummaryIndex.flush(write.cacheKey());

        // read back through a different instance
        PersistingSummaryIndex readIndex = new PersistingSummaryIndex("build/SIDX");
        List<Bucket> read1 = readIndex.read(read.cacheKey(), read.getStartTimeMs(), read.getEndTimeMs());
        assertThat(read1.size(), is(equalTo(1)));
    }

    @Test
    public void testAggregate() throws Exception {

        PersistingSummaryIndex persistingSummaryIndex = new PersistingSummaryIndex("build/SIDX-STRESS");
        LogRequest write = new LogRequestBuilder().getLogRequest("sub", Arrays.asList("* | _filename.count() summary.index(write)"), "", new DateTime().minusHours(24).getMillis(), new DateTime().getMillis());
        LogRequest delete = new LogRequestBuilder().getLogRequest("sub", Arrays.asList("* | _filename.count() summary.index(delete)"), "", new DateTime().minusHours(24).getMillis(), new DateTime().getMillis());
        LogRequest read = new LogRequestBuilder().getLogRequest("sub", Arrays.asList("* | _filename.count() summary.index(read)"), "", new DateTime().minusHours(24).getMillis(), new DateTime().getMillis());
        persistingSummaryIndex.delete(delete.cacheKey(), null, "", write);

        // check the write bucket Width is 10 minutes
        assertEquals(10 * DateUtil.MINUTE, write.getBucketWidthSecs() * 1000);

        int hits = 500;
        for (int i =0; i < hits; i++) {

            // write a real bucket for persisting
            Bucket bucket = new Bucket(write.getStartTimeMs(), write.getStartTimeMs() + 1000, write.query(0).functions(), 0, "*", "source", "sub", "");

            String hostname = "host";
            String resource = "source";
            FieldSet fieldSet = FieldSets.getBasicFieldSet();
            fieldSet.addDefaultFields("log4j", hostname, "file.log", "/var/log/file.log", "log4j", "Agent", resource, 0, true);
            long time = System.currentTimeMillis();
            bucket.increment(fieldSet, new String[]{"line"}, "file.log", "/var/log/file.log", time, time, time, 100, "line", new MatchResult("file"), false, time, time);

            persistingSummaryIndex.write(write.cacheKey(), bucket);
        }


        persistingSummaryIndex.flush(write.cacheKey());

        // read back through a different instance
        PersistingSummaryIndex readIndex = new PersistingSummaryIndex("build/SIDX-STRESS");
        List<Bucket> read1 = readIndex.read(read.cacheKey(), read.getStartTimeMs(), read.getEndTimeMs());
        assertThat(read1.size(), is(equalTo(1)));
        assertThat(read1.get(0).hits(), is(equalTo(hits)));

        Count count = (Count) read1.get(0).functions().iterator().next();
        Map results = count.getResults();
        IntValue counted = (IntValue) results.get("file.log");
        assertThat(counted.value(), is(equalTo(hits)));


    }
}