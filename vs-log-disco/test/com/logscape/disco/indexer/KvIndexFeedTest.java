package com.logscape.disco.indexer;

import com.logscape.disco.kv.RulesKeyValueExtractor;
import com.logscape.disco.grokit.GrokItPool;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 17/09/2013
 * Time: 15:30
 * To change this template use File | Settings | File Templates.
 */
public class KvIndexFeedTest {


    private RulesKeyValueExtractor kv;
    private KvIndexFeed kvIndexFeed;
    private KvIndex kvIndex;
    private LookUpTable lookUpTable;
    private String filename = "";

    @Before
    public void setUp() throws Exception {
        kv = mock(RulesKeyValueExtractor.class);
        kvIndex = mock(KvIndex.class);
        lookUpTable = mock(LookUpTable.class);
        Dictionary dictionary = mock(Dictionary.class);
        kvIndexFeed = new KvIndexFeed(kv, kvIndex, lookUpTable, dictionary, new GrokItPool(), null, null);
    }

    @Test
    public void shouldSmokeItUp() throws Exception {
        IndexFeed feed = KvIndexFactory.get();
        feed.index(1, filename, 1, 1, "2013-09-18 10:29:11,702 INFO pool-2-thread-1 (license.TrialListener)\t Action:'Download' Email:'izam.my@gmail.com' IpAddress:'175.143.7.68' Company:'sadasd'", false, false, true, null);
        List<Pair> fieldIs = feed.get(1, 1);

    }

    @Test
    public void shouldExtractKvs() throws Exception {
        String data = " foo=blah";
        kvIndexFeed.index(1, filename, 1, 1, data, true, false, true, null);
        verify(kv).getFields(data);
    }

    @Test
    public void shouldNormalizeViaLookupTable() throws Exception {
        List<Pair> fieldIs = Collections.<Pair>singletonList(new Pair("foo", "value"));
        when(kv.getFields(anyString())).thenReturn(fieldIs);
        List<Pair> fields = kvIndexFeed.index(1, filename, 1, 1, "someshit", true, false, false, null);
        lookUpTable.normalize(1, fields);
        verify(lookUpTable).normalize(1, fieldIs);
    }

    @Test
    public void shouldIndexNormalizedData() throws Exception {
        List<Pair> fieldIs = Collections.<Pair>singletonList(new Pair("foo","value"));
        String[] normalized = "abc,def,ghi".split(",");
        when(kv.getFields(anyString())).thenReturn(fieldIs);
        when(lookUpTable.normalize(anyInt(), anyListOf(Pair.class))).thenReturn(normalized);
        List<Pair> fields = kvIndexFeed.index(1, filename, 1, 1, "whateva-data", true, false, false, null);
        kvIndexFeed.store(1, 1, fields);


        verify(kvIndex).index(1,1,normalized);
    }

    @Test
    public void shouldRemoveDataFromIndex() {
        kvIndexFeed.remove(10);
        verify(lookUpTable).remove(10);
        verify(kvIndex).remove(10);
    }
}
