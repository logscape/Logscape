package com.liquidlabs.log.search.functions.txn;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.liquidlabs.log.search.functions.ValueSetter;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TxnFunTest {

    Map<String, List<Number>> setted = new HashMap<String, List<Number>>();


    @Before
    public void before() {
        setted = new HashMap<String, List<Number>>();
    }

    @Test
    public void shouldSplitTheLongOne() throws Exception {
        Map<String, IntArrayList> txns = new HashMap<String, IntArrayList>();

        // txn times offset from the start time of the request
        txns.put("xxx1", IntArrayList.from(0, 500, 1000));
        txns.put("xxx2", IntArrayList.from(0, 1000, 2500));
        txns.put("xxx3", IntArrayList.from(0, 1000, 2500));
        txns.put("xxx4", IntArrayList.from(0, 1000, 2500));
        txns.put("sameBucketFast", IntArrayList.from(500, 550, 600));
        txns.put("SLOW", IntArrayList.from(500, 10000, 50000, 51000));
        SyntheticTransAccumulate txn = new SyntheticTransAccumulate();
        Map<String, IntArrayList> results = txn.splitLongTxns(txns);
        System.out.println(results);

    }


    @Test
    public void shouldDoRatePerf() throws Exception {
        RatePerf func = new RatePerf(10);
        Map<String, IntArrayList> txns = new HashMap<String, IntArrayList>();

        // txn times offset from the start time of the request
        txns.put("xxx1", IntArrayList.from(0, 500, 1000));
        txns.put("xxx2", IntArrayList.from(0, 1000, 2500));
        txns.put("sameBucketFast", IntArrayList.from(500, 550, 600));

        applyDivisor(txns);

        ValueSetter valueSetter = new ValueSetter() {
            public void set(String group, Number value, boolean crap) {
                if (!setted.containsKey(group)) setted.put(group, new ArrayList<Number>());
                setted.get(group).add(value);
            }
        };
        func.apply(0, txns, valueSetter , 0, 1000, 0, null);
        func.apply(1, txns, valueSetter , 1001, 2000, 0, null);
        func.apply(2, txns, valueSetter , 2001, 3000, 0, null);
        System.out.println(setted);
        // we dont get the time info in the setter... bit crappy- need to pass it through to see it work properly
        assertEquals("{Max=[1000, 0, 2500], Avg=[550, 0, 2500], Min=[100, 0, 2500]}", setted.toString());

    }

    @Test
    public void shouldShowTxnRate() throws Exception {
        Rate func = new Rate(10);
        Map<String, IntArrayList> txns = new HashMap<String, IntArrayList>();

        // txn times offset from the start time of the request
        txns.put("xxx1", IntArrayList.from(0, 1000, 1500));
        txns.put("xxx2", IntArrayList.from(500, 1000, 2500));
        txns.put("sameBucketFast", IntArrayList.from(500, 550, 600));

        applyDivisor(txns);

        ValueSetter valueSetter = new ValueSetter() {
            public void set(String group, Number value, boolean crap) {
                if (!setted.containsKey(group)) setted.put(group, new ArrayList<Number>());
                setted.get(group).add(value);
            }
        };
        func.apply(0, txns, valueSetter , 0, 1000, 0, null);
        func.apply(1, txns, valueSetter , 1001, 2000, 0, null);
        func.apply(2, txns, valueSetter , 2001, 3000, 0, null);
        System.out.println(setted);
        // we dont get the time info in the setter... bit crappy- need to pass it through to see it work properly
        assertEquals("{Rate-Trend=[3], Rate=[3, 2, 1]}", setted.toString());
    }

    @Test
    public void shouldMapElapsedTimesFun() throws Exception {

        DateTime runTime = new DateTime().minusSeconds(5);

        Elapsed func = new Elapsed(5);
        Map<String, IntArrayList> txns = new HashMap<String, IntArrayList>();

        // txn times offset from the start time of the request
        txns.put("xxx1", IntArrayList.from(0, 1000, 1500));
        txns.put("xxx2", IntArrayList.from(1500, 1800, 2500));
        txns.put("sameBucketFast", IntArrayList.from(500, 550, 600));

        applyDivisor(txns);

        ValueSetter valueSetter = new ValueSetter() {
            public void set(String group, Number value, boolean crap) {
                if (!setted.containsKey(group)) setted.put(group, new ArrayList<Number>());
                setted.get(group).add(value);
            }
        };
        func.apply(0, txns, valueSetter , runTime.getMillis(), runTime.plusSeconds(1).getMillis(),  runTime.getMillis(), null);
        func.apply(1, txns, valueSetter , runTime.plusSeconds(1).getMillis()+1, runTime.plusSeconds(2).getMillis(),  runTime.getMillis(), null);
        func.apply(2, txns, valueSetter , runTime.plusSeconds(2).getMillis()+1, runTime.plusSeconds(3).getMillis(),  runTime.getMillis(), null);
        System.out.println(setted);
        // we dont get the time info in the setter... bit crappy- need to pass it through to see it work properly
        assertEquals("{xxx1_1500.0MS=[1500.0, 1500.0], sameBucketFast_100.0MS=[100.0, 100.0], xxx2_1000.0MS=[1000.0, 1000.0]}", setted.toString());

    }

    @Test
    public void shouldDoFast() throws Exception {
        Fastest func = new Fastest(10);
        Map<String, IntArrayList> txns = new HashMap<String, IntArrayList>();

        // txn times offset from the start time of the request
        txns.put("xxx1", IntArrayList.from(0, 1000, 2000));
        txns.put("xxxFAST", IntArrayList.from(0, 1000, 1800));
        txns.put("xxx2", IntArrayList.from(0, 1000, 2100));

        applyDivisor(txns);

        ValueSetter valueSetter = new ValueSetter() {
            public void set(String group, Number value, boolean crap) {
                if (!setted.containsKey(group)) setted.put(group, new ArrayList<Number>());
                setted.get(group).add(value);
            }
        };
        func.apply(0, txns, valueSetter , 0, 1000, 0, null);
        func.apply(1, txns, valueSetter , 0, 1000, 0, null);
        func.apply(2, txns, valueSetter , 0, 1000, 0, null);
        func.apply(3, txns, valueSetter , 0, 1000, 0, null);

        // 3 hops - 1second between each
        Assert.assertEquals("{Fastest-xxxFAST=[0, 1000, 1800]}", setted.toString());
    }

    @Test
    public void shouldDoSlow() throws Exception {
        Slowest func = new Slowest(10);
        Map<String, IntArrayList> txns = new HashMap<String, IntArrayList>();

        // txn times offset from the start time of the request
        txns.put("xxx1", IntArrayList.from(0, 1000, 2000));
        txns.put("xxxSLOW", IntArrayList.from(0, 1000, 2500));
        txns.put("xxx2", IntArrayList.from(0, 1000, 2100));

        applyDivisor(txns);

        ValueSetter valueSetter = new ValueSetter() {
            public void set(String group, Number value, boolean crap) {
                if (!setted.containsKey(group)) setted.put(group, new ArrayList<Number>());
                setted.get(group).add(value);
            }
        };
        func.apply(0, txns, valueSetter , 0, 1000, 0, null);
        func.apply(1, txns, valueSetter , 0, 1000, 0, null);
        func.apply(2, txns, valueSetter , 0, 1000, 0, null);
        func.apply(3, txns, valueSetter , 0, 1000, 0, null);

        // 3 hops - 1second between each
        Assert.assertEquals("{Slowest-xxxSLOW=[0, 1000, 2500]}", setted.toString());
    }

    @Test
    public void shouldDoAvg() throws Exception {
        Avg func = new Avg(10);
        Map<String, IntArrayList> txns = new HashMap<String, IntArrayList>();

        // txn times offset from the start time of the request
        txns.put("xxx1", IntArrayList.from(0, 1000, 2000));
        // txn times offset from the start time of the request
        txns.put("xxx2", IntArrayList.from(0, 1000, 2000));

        applyDivisor(txns);

        ValueSetter valueSetter = new ValueSetter() {
            public void set(String group, Number value, boolean crap) {
                if (!setted.containsKey(group)) setted.put(group, new ArrayList<Number>());
                setted.get(group).add(value);
            }
        };
        func.apply(0, txns, valueSetter , 0, 1000, 0, null);
        func.apply(1, txns, valueSetter , 0, 1000, 0, null);
        func.apply(2, txns, valueSetter , 0, 1000, 0, null);
        func.apply(3, txns, valueSetter , 0, 1000, 0, null);

        // 3 hops - 1second between each
        Assert.assertEquals("{Avg=[0, 1000, 2000]}", setted.toString());
    }
    private void applyDivisor(Map<String, IntArrayList> txns) {
        HashMap<String, IntArrayList> newMap = new HashMap<String, IntArrayList>();

        for (String s : txns.keySet()) {
            IntArrayList intCursors = txns.get(s);
            IntArrayList newList = new IntArrayList();
            for (IntCursor intCursor : intCursors) {
                newList.add(intCursor.value/SyntheticTransAccumulate.DIVISOR);
            }
            newMap.put(s, newList);
        }

        txns.putAll(newMap);
    }

}
