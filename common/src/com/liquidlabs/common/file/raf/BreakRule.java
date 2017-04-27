package com.liquidlabs.common.file.raf;


public interface BreakRule {

    boolean isLineBreakable();

    public enum Rule { Default, SingleLine,  Year, Month, MonthNumeric, Explicit, DayNumeric };

    boolean isKeepReading(byte c0, byte c1, byte c2, byte c3);
    boolean isKeepReading(String bytes);
    String explicitBreak();
    int getCharsLength();
    int breakLength();
    boolean isExplicitRule();

}
