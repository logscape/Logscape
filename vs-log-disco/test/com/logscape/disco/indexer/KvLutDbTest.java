package com.logscape.disco.indexer;

import com.logscape.disco.indexer.mapdb.MapDb;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 17/09/2013
 * Time: 16:04
 * To change this template use File | Settings | File Templates.
 */
public class KvLutDbTest {

    private MapDb mapDb;
    private KvLutDb lut;
    private Dictionary dictionary;

    @Before
    public void setUp() throws Exception {
        mapDb = mock(MapDb.class);
        dictionary = mock(Dictionary.class);
        lut = new KvLutDb(mapDb, dictionary);
    }

    @Test
    public void shouldLookupLut() throws Exception {
        lut.normalize(1, Collections.<Pair>singletonList(new Pair("foo","value")));
        verify(mapDb).get(1);
    }

    @Test
    public void shouldUpdateLut() throws Exception {
        when(mapDb.get(1)).thenReturn(new String[] { "blah" });
        lut.normalize(1, Collections.<Pair>singletonList(new Pair("foo","value")));
        verify(mapDb).put(1, "blah,foo".split(","));
    }

    @Test
    public void shouldReturnListOfValues() throws Exception {
        when(mapDb.get(1)).thenReturn(new String[] { "0","blah" });
        String[] result = lut.normalize(1, Collections.<Pair>singletonList(new Pair("foo", "value")));
        assertThat(result[1], is("value"));
    }

//    @Test
//    public void shouldGetStuffFromDB() throws Exception {
//        when(mapDb.get(1)).thenReturn("fooKey,blahKey".split(","));
//        List<FieldI> expected = new ArrayList();
//        expected.add(new LiteralField("fooKey", 1, true, true, "fooValue", "count"));
//        expected.add(new LiteralField("blahKey", 1, true, true, "blahValue", "count"));
//
//
//        HashMap<Integer, CompactCharSequence> dictionaryMap = new HashMap<Integer, CompactCharSequence>();
//        dictionaryMap.put(0, new CompactCharSequence("fooValue"));
//        dictionaryMap.put(1, new CompactCharSequence("blahValue"));
//        when(dictionary.get(1)).thenReturn(dictionaryMap);
//
//
//        List<FieldI> results = lut.get(1, new Integer[] { 0,0, 1,1 });
//        assertThat(results, is(expected));
//    }
//    @Test
//    public void shouldGetStuffFromDBWithOffset() throws Exception {
//        when(mapDb.get(1)).thenReturn("fooKey,blahKey".split(","));
//        List<FieldI> expected = new ArrayList();
//        //expected.add(new LiteralField("fooKey", 1, true, true, "fooValue", "count"));
//        expected.add(new LiteralField("blahKey", 1, true, true, "blahValue", "count"));
//
//        HashMap<Integer, CompactCharSequence> dictionaryMap = new HashMap<Integer, CompactCharSequence>();
//        dictionaryMap.put(1, new CompactCharSequence("blahValue"));
//        when(dictionary.get(1)).thenReturn(dictionaryMap);
//
//        List<FieldI> results = lut.get(1, new Integer[] { 1,1 });
//        assertThat(results, is(expected));
//    }

    @Test
    public void shouldRemoveStuff() {
       lut.remove(90);
       verify(mapDb).remove(90);
    }



}
