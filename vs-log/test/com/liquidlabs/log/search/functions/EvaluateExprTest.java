package com.liquidlabs.log.search.functions;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Created by neil.avery on 29/12/2015.
 */
public class EvaluateExprTest {

    @Test
    public void testHandleSingleValue() throws Exception {

        //  eval(_EACH_ / 1024 / 1024)
        EvaluateExpr eval = new EvaluateExpr("tag", EvaluateExpr.EACH + " / 1024.0 / 1024.0", "0");
        Map<String, Object> values = new HashMap<>();
        values.put("AA", "100.1");
        final AtomicInteger hits = new AtomicInteger();

        eval.handle(values, new ValueSetter() {
            @Override
            public void set(String group, Number value, boolean useThisResultLiterally) {
                assertEquals("AA", group);
                System.out.println(group + ":" + value);
                hits.incrementAndGet();
            }
        });

        assertEquals(1, hits.get());

    }

    @Test
    public void testHandleMultiSingleValue() throws Exception {

        //  eval(_EACH_ / 1024 / 1024)
        EvaluateExpr eval = new EvaluateExpr("tag", EvaluateExpr.EACH + " * 1024", "0");
        Map<String, Object> values = new HashMap<>();
        values.put("AA", "100.1");
        values.put("BB", "200.1");
        final AtomicInteger hits = new AtomicInteger();

        eval.handle(values, new ValueSetter() {
            @Override
            public void set(String group, Number value, boolean useThisResultLiterally) {
                System.out.println(group + ":" + value);
                hits.incrementAndGet();
            }
        });

        assertEquals(2, hits.get());

    }
}