package com.liquidlabs.log.server;

import com.liquidlabs.common.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

public class ChunkOfFileHandlerTest {

    private StringWriter writer;
    private ChunkOfFileHandler chunkOfFileHandler;
    private String theTemplate = "";

    @Before
    public void setUp() throws Exception {
        writer = new StringWriter();
        chunkOfFileHandler = new ChunkOfFileHandler(writer, new LogLineOutput(writer), new File("test-data/agent.log"), 1);
    }

    @Test
    public void shouldReturnNextLineAndPos() throws IOException {
        Pair<Long,Long> upTo = chunkOfFileHandler.nextChunk(theTemplate, 1, 0, 1);
        assertThat(upTo._1, is(2L));
        assertThat(upTo._2, is(greaterThan(0L)));
    }

    @Test
    public void shouldWriteUpToPageSizeDataToWriter() throws IOException {
        chunkOfFileHandler.nextChunk(theTemplate, 1, 0, 1);
        assertThat(writer.toString(), is("<span class='alert-success' line='1'>2008-11-11 17:02:19,538 INFO main (UDPServer.java:230)\t - mcast://225.0.0.0:6000/[] Adding Peer:mcast://225.0.0.0:6000</span>\n"));
    }

    @Test
    public void shouldGetCorrectChunks() throws IOException {
        Pair<Long,Long> upTo = chunkOfFileHandler.nextChunk(theTemplate, 1, 0, 1);
        final Pair<Long, Long> next = chunkOfFileHandler.nextChunk(theTemplate, upTo._1, upTo._2, 1);
        assertThat(next._1, is(3L));
        assertThat(next._2, is(greaterThan(upTo._2)));
        assertThat(writer.toString(), is(endsWith("<span class='alert-success' line='2'>2008-11-11 17:02:19,542 INFO main (MulticastDiscovery.java:53)\t - MulticastDiscovery -8224184706970019453 address[] filters{} Discovery Starting with freq:5</span>\n")));
    }

    @Test
    public void shouldWriteNothingAndReturnMinus1sIfAskingForStuffBeyondEndOfFile() throws IOException {
        final Pair<Long, Long> next = chunkOfFileHandler.nextChunk(theTemplate, 1, 999999, 1);
        assertThat(next._1, is(-1L));
        assertThat(next._2, is(-1L));
    }
}
