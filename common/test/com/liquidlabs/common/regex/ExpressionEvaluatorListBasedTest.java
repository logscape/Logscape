package com.liquidlabs.common.regex;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 05/08/2013
 * Time: 12:20
 * To change this template use File | Settings | File Templates.
 */
public class ExpressionEvaluatorListBasedTest {
    @Test
    public void testEvaluateContains() throws Exception {

        List<ExpressionEvaluatorListBased.Item> expr = new ArrayList<ExpressionEvaluatorListBased.Item>();
        expr.add(new ExpressionEvaluatorListBased.Item(new String[] { "one"}));
        expr.add(new ExpressionEvaluatorListBased.Item(new String[]{"two"}));
        ExpressionEvaluatorListBased ee = new ExpressionEvaluatorListBased(expr);
        assertTrue(ee.evaluate("one two"));
        assertFalse(ee.evaluate("one three"));

    }

//    @Test - not doing this at the moment
    public void testEvaluateNot() throws Exception {

        List<ExpressionEvaluatorListBased.Item> expr = new ArrayList<ExpressionEvaluatorListBased.Item>();
//        expr.add(new ExpressionEvaluatorListBased.Item(false, new String[] { "one"}));
//        expr.add(new ExpressionEvaluatorListBased.Item(false, new String[]{"two"}));
        ExpressionEvaluatorListBased ee = new ExpressionEvaluatorListBased(expr);
        assertTrue(ee.evaluate("yay yay"));
        assertFalse(ee.evaluate("one three"));

    }


}
