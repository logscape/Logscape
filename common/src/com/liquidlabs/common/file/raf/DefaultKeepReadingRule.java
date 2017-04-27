package com.liquidlabs.common.file.raf;

import gnu.trove.map.hash.TIntObjectHashMap;
import org.joda.time.DateTime;

import java.text.DateFormatSymbols;

public class DefaultKeepReadingRule implements BreakRule {

    static public BreakRule getRule(String breakRule) {
        if (breakRule == null || breakRule.trim().length() == 0) return new DefaultKeepReadingRule();
        if (breakRule.equals(BreakRule.Rule.Year.name())) {
            DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
            rule.setYearBased();
            return rule;
        }
        if (breakRule.equals(BreakRule.Rule.Month.name())) {
            DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
            rule.setMonthCharBased();
            return rule;
        }
        if (breakRule.equals(BreakRule.Rule.MonthNumeric.name())) {
            DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
            rule.setMonthNumeric();
            return rule;
        }
        if (breakRule.equals(BreakRule.Rule.DayNumeric.name())) {
            DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
            rule.setDayNumeric();
            return rule;
        }
        if (breakRule.contains(BreakRule.Rule.Explicit.name())) {
            DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
            rule.setExplicitBreak(breakRule.substring(breakRule.indexOf(":")+1));
            rule.setExplicit(true);
            rule.lineBreakable = false;
            return rule;
        }
        DefaultKeepReadingRule rule = new DefaultKeepReadingRule();
        return rule;

    }
    boolean lineBreakable = true;

    @Override
    public boolean isLineBreakable() {
        return lineBreakable;
    }

    protected static final char KEEP_READING_CHARS[] = new char[] { ' ', '\t' };

    // break line rules --- 2010? etc
    // 2010 OR Month OR Hour]/
    char[][] breakOnThese =  { } ;// "2010".getBytes() };//, "Oct".getBytes(), "15".getBytes() };
    TIntObjectHashMap breakOnTheseMap = new TIntObjectHashMap();

    @Override
    public int breakLength() {
        if (breakOnThese.length == 0) return 1;
        return breakOnThese[0].length;
    }
    public boolean isExplicitRule() {
        return explicit;
    }
    public boolean isKeepReading(byte b0, byte b1, byte b2, byte b3) {
        return isKeepReading(new String(new byte[] { b0, b1, b2, b3 }));
    }

    public boolean isKeepReading(String bytes) {
        if (bytes.length() == 0) {
            // nothing more to reaf but we also dont look ahead - so dont keep reading
            if (breakOnThese.length == 0) return false;
                // a break was specified but need more info - keep reading
            else return  true;
        }
        if (!explicit) {
            for (char c : KEEP_READING_CHARS) {
                if (c == bytes.charAt(0)) return true;
            }
        }

        // no rules set - break
        if (breakOnThese.length == 0) return false;

//        Still broken for some tests.... leaving it for now
//        int breakHash = getBreakHash(bytes, breakOnThese[0]);
//        if (breakHash == -1) return false;
//        if (true) {
//            return ! breakOnTheseMap.containsKey(breakHash);// != null)
//        }



        // break when we get a match
        for (char[] breakOnMatch : breakOnThese) {
            if (equals(bytes, breakOnMatch)) {
                return false;
            }
            // if we matched an entry - then make it break
        }


        return true;
    }


    private boolean equals(String bytes, char[] breakOnMatch) {
        if (bytes.length() >= 2 && breakOnMatch.length >= 2) {
            if (bytes.charAt(0) != breakOnMatch[0] || bytes.charAt(1) != breakOnMatch[1]) return false;
        }
        int length = Math.min(bytes.length(), breakOnMatch.length);
        for (int pos = 0; pos < length; pos++) {
            if (breakOnMatch[pos] != bytes.charAt(pos)) {
                return false;
            }
        }

        return true;
    }

    public void setYearBased() {
        StringBuilder yearString = new StringBuilder();
        for (int i = new DateTime().getYear() - 5; i < new DateTime().getYear() + 3; i++) {
            yearString.append(i).append(",");

        }
        setExplicitBreak(yearString.toString());
    }
    public void setMonthNumeric() {
        StringBuilder string = new StringBuilder();
        for (int i = 0; i < 13; i++) {
            string.append(String.format("%02d", i)).append(",");
            string.append(String.format("%d", i)).append(",");
        }
        setExplicitBreak(string.toString());
    }
    public void setDayNumeric() {
        StringBuilder string = new StringBuilder();
        for (int i = 0; i < 31; i++) {
            string.append(String.format("%02d", i)).append(",");
            string.append(String.format("%d", i)).append(",");
        }
        setExplicitBreak(string.toString());
    }

    public void setMonthCharBased() {
        String[] months = new DateFormatSymbols().getMonths();
        StringBuilder monthsString = new StringBuilder();

        for (String month : months) {
            if (month.trim().length() == 0) continue;
            monthsString.append(month.substring(0, 3)).append(",");
        }
        setExplicitBreak(monthsString.toString());

    }
    public int getCharsLength() {
        return breakOnThese[0].length;
    }

    boolean explicit = false;
    public void setExplicit(boolean value) {
        this.explicit = value;
    }

    String explicitBreakString = "";

    public void setExplicitBreak(String string) {
        String[] tokens = string.split(",");
        explicitBreakString = string;
        breakOnThese = new char[tokens.length][];
        int pos = 0;
        for (String token : tokens) {
            breakOnThese[pos] = token.toCharArray();
            breakOnTheseMap.put(getBreakHash(token, breakOnThese[pos]), breakOnThese[pos]);
            pos++;
        }
        explicit = false;
    }
    public String explicitBreak() {
        return this.explicitBreakString;
    }
    private int getBreakHash(String bytes, char[] bytes1) {
        if (bytes.length() < bytes1.length) return -1;
        int h = 1;
        for (int i = bytes1.length-1; i >= 0; i--)
            h = 31 * h + (int)bytes.charAt(i);
        return h;

    }
    private int getBreakHash(char[] bytes1) {
        int h = 1;
        for (int i = bytes1.length-1; i >= 0; i--)
            h = 31 * h + (int)bytes1[i];
        return h;

    }



    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("KeepReadingRule:");
        char[][] breakOnThese2 = breakOnThese;
        for (char[] bs : breakOnThese2) {
            String line = new String(bs);
            stringBuilder.append(" ").append(line);
        }

        return stringBuilder.toString();
    }

}
