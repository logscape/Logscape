package com.liquidlabs.common.file;

import static org.junit.Assert.*;

import com.liquidlabs.common.file.raf.DefaultKeepReadingRule;
import org.junit.Test;

public class DefaultKeepReadingRuleTest {

    @Test
    public void shouldDetectNLAngle() throws Exception {
        DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
        rule.setExplicitBreak("\n<");
        assertTrue(rule.isKeepReading("one"));
        assertTrue(rule.isKeepReading("two"));
        assertFalse(rule.isKeepReading("\n<"));
    }

    @Test
    public void shouldDetectNLNL() throws Exception {
        DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
        rule.setExplicitBreak("\n\n");
        assertTrue(rule.isKeepReading("one"));
        assertTrue(rule.isKeepReading("two"));
        assertFalse(rule.isKeepReading("\n\n"));
    }

    @Test
    public void shouldHandleExplicitNewLineRule() throws Exception {
        DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
        rule.setExplicitBreak("Event,Me,#");
        assertFalse(rule.isKeepReading("Event stuff"));
        assertFalse(rule.isKeepReading("Me stuff"));
        assertFalse(rule.isKeepReading("#<stuff"));
        assertTrue(rule.isKeepReading("blah stuff"));
    }
    @Test
    public void shouldHandleExplicitNewLineRule2() throws Exception {
        DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
        rule.setExplicitBreak("\n");
        assertTrue(rule.isKeepReading("one"));
        assertTrue(rule.isKeepReading("two"));
        assertFalse(rule.isKeepReading("\n"));
    }
    @Test
    public void shouldDoYearBreakOnLine() throws Exception {
        DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
        rule.setYearBased();

        assertFalse(rule.isKeepReading("2019 stuff"));
        assertFalse(rule.isKeepReading("2018 stuff"));
        assertFalse(rule.isKeepReading("2017 stuff"));

        assertTrue(rule.isKeepReading("2007 stuff"));
        assertTrue(rule.isKeepReading("stuff"));

    }
    @Test
    public void shouldDoMonthNumericOfYear() throws Exception {
        DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
        rule.setMonthNumeric();

        assertFalse(rule.isKeepReading("0/03 stuff"));
        assertFalse(rule.isKeepReading("01 stuff"));
        assertFalse(rule.isKeepReading("1/04 stuff"));
        assertFalse(rule.isKeepReading("2 stuff"));
        assertFalse(rule.isKeepReading("9 stuff"));
        assertFalse(rule.isKeepReading("10 stuff"));
        assertFalse(rule.isKeepReading("11/03 stuff"));
        assertFalse(rule.isKeepReading("12 stuff"));

        assertTrue(rule.isKeepReading("zzz stuff"));
        assertTrue(rule.isKeepReading("stuff"));

    }
    @Test
    public void shouldDoDayNumericOfYear() throws Exception {
        DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
        rule.setDayNumeric();

        assertFalse(rule.isKeepReading("0/03 stuff"));
        assertFalse(rule.isKeepReading("01 stuff"));
        assertFalse(rule.isKeepReading("1/04 stuff"));
        assertFalse(rule.isKeepReading("2 stuff"));
        assertFalse(rule.isKeepReading("9 stuff"));
        assertFalse(rule.isKeepReading("10 stuff"));
        assertFalse(rule.isKeepReading("11/03 stuff"));
        assertFalse(rule.isKeepReading("12 stuff"));

        assertTrue(rule.isKeepReading("zzz stuff"));
        assertTrue(rule.isKeepReading("stuff"));

    }
    @Test
    public void shouldDoMonthOfYear() throws Exception {
        DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
        rule.setMonthCharBased();

        assertFalse(rule.isKeepReading("Jan stuff"));
        assertFalse(rule.isKeepReading("Feb stuff"));
        assertFalse(rule.isKeepReading("Oct stuff"));
        assertFalse(rule.isKeepReading("Nov stuff"));
        assertFalse(rule.isKeepReading("Dec stuff"));

        assertTrue(rule.isKeepReading("zzz stuff"));
        assertTrue(rule.isKeepReading("stuff"));

    }

}
