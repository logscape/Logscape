package com.liquidlabs.common.file.raf;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.file.FileUtil;
import org.joda.time.DateTime;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class BreakRuleUtil {
	
	private static final String _02D = "%02d";
    private static List<String> monthNumericStarters;

    public static String[] getLinesFromTextBlock(String text) {
        boolean isAlreadyDoubleSpaced = StringUtil.isDoubleSpaced(text);
        List<String> testlines = null;

        // if we have more than 1 zero length line...
        if (isAlreadyDoubleSpaced) {
            testlines = chunkLines(text);
        } else {
            List<String> lineItems = getLineItemsUser(text);
            // now figure out the line break rule... unless it was manually split for us....
            String detectedBreakRule = BreakRuleUtil.getStandardNewLineRule(lineItems, BreakRule.Rule.Default.name(), "");
            File file = null;
            try {
                file = dumptoTempFile(lineItems);
                testlines = FileUtil.readLines(file.getPath(), Integer.MAX_VALUE, detectedBreakRule);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (file != null) file.delete();
            }

        }
        return  com.liquidlabs.common.collection.Arrays.toStringArray(testlines);
    }

    static List<String> getLineItemsUser(String text) {
        List<String> lineItems = new ArrayList<String>();
        String[] massagedLines = text.toString().split("\n");
        if (massagedLines.length == 1) massagedLines = text.split("\r");
        for (String line : massagedLines) {
            if (line.trim().length() > 0) {
                lineItems.add(line);
            }
        }
        return lineItems;
    }

    /**
     * User has double spaced the lines for us...
     * @param text
     * @return
     */
    static List<String> chunkLines(String text) {
        String[] items = text.split("\n\n");
        //if (items.length == 1 && text.contains("\r")) items = text.split("\r");

        List<String> results = new ArrayList<String>();
        StringBuilder line = new StringBuilder();
        for (String string : items) {
            results.add(string);
//            if (string.trim().length() == 0) {
//                results.add(line.toString());
//                line.delete(0, line.length());
//
//            } else {
//                if (string.startsWith("#")) {
//                    results.add(string);
//                } else {
//                    line.append(string);
//                    line.append("\n");
//                }
//            }
        }
//        if (line.length() > 0) results.add(line.toString());

        return results;
    }

    static File dumptoTempFile(List<String> lineItems) throws IOException, FileNotFoundException {
        // now rebuild the lines using the break rule...
        File createTempFile = File.createTempFile("brule", "log");
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(createTempFile)));
        for (String line : lineItems) {
            writer.write(line);
            writer.newLine();
        }
        writer.close();
        return createTempFile;
    }


    static public String getStandardNewLineRule(List<String> content, String defaultRule, String timeFormat) {
        if (timeFormat.startsWith("yyyy")) return BreakRule.Rule.Year.name();
        if (defaultRule == null || defaultRule.length() == 0) defaultRule = BreakRule.Rule.Default.name();
        boolean isGuessing = defaultRule.equalsIgnoreCase(BreakRule.Rule.Default.name()) || defaultRule.equalsIgnoreCase(BreakRule.Rule.SingleLine.name());
        if (defaultRule != null && !isGuessing) return defaultRule;
        DateTime dateTime = new DateTime();
        DateTime.Property month = dateTime.monthOfYear();
        String monthString = month.getAsShortText().toUpperCase();
        // now fall back to any of the 12 months in the year
        int curYear = dateTime.getYear();
        List<String> years = new ArrayList<>();
        for(int i = 0; i < 10; i++) years.add((curYear-i)+"");


        int yearVote = 0;
        int monthNumericVote = 0;
        int monthVote = 0;
        int dayNumericVote = 0;
        for (String line : content) {
            for(String year : years){
                if(line.startsWith(year)) yearVote++;
                break;
            }
            if (line.startsWith(dateTime.getMonthOfYear() + "") ) {
                // see if the next one is a non-numeric
                boolean nextPos1Numeric = line.charAt(1) >= 0 || line.charAt(1) <= 9;
                boolean nextPos2Numeric = line.charAt(2) >= 0 || line.charAt(2) <= 9;
                if (nextPos1Numeric == false || nextPos2Numeric == false)   monthNumericVote++;//return MONTH_NUMERIC;
            } else if (line.startsWith(dateTime.getDayOfMonth() + "/")) {
                boolean nextPos1Numeric = line.charAt(1) >= 0 || line.charAt(1) <= 9;
                boolean nextPos2Numeric = line.charAt(2) >= 0 || line.charAt(2) <= 9;
                if (nextPos1Numeric == false || nextPos2Numeric == false)      dayNumericVote++;//return DAY_NUMERIC;
            } else if (line.length() > 3){
                if (line.substring(0, 3).toUpperCase().startsWith(monthString)) monthVote++;//return MONTH;
            }
        }
        if (yearVote > 0 && monthNumericVote == 0 && monthVote == 0) return BreakRule.Rule.Year.name();
        if (monthNumericVote > 0 && yearVote == 0 && monthVote == 0) return BreakRule.Rule.MonthNumeric.name();
        if (dayNumericVote > 0 && yearVote == 0 && monthVote == 0) return BreakRule.Rule.DayNumeric.name();
        if (monthVote > 0 && yearVote == 0 && monthNumericVote == 0) return BreakRule.Rule.Month.name();
        monthVote = 0;
        monthNumericVote = 0;
        int thirdCharIsNumber = 0;
        List<String> monthNumericStarters = getMonthNumericStarters();
        List<String> dayNumericStarters = getDayNumericStarters();
        for (String line : content) {
            if (line.length() < 6) continue;
            if (lineStartsWithMonthString(line)) {
                monthVote++;
            }
            for (String monthNumericStarter : monthNumericStarters) {
                if (line.startsWith(monthNumericStarter)) monthNumericVote++;
            }
            for (String dayNumericStarter : dayNumericStarters) {
                if (line.startsWith(dayNumericStarter)) dayNumericVote++;
            }

            if (StringUtil.isIntegerFast("" + line.charAt(3))) thirdCharIsNumber++;

        }
        // confused so need to look at second secontion...
        if (monthNumericVote > 1 && dayNumericVote > 1) {
            if (thirdCharIsNumber > 0) {
                int day2Vote = 0;
                int month2Vote = 0;
                for (String line : content) {
                    if (line.length() < 6) continue;
                    String nextBit = line.substring(3, 5);
                    Integer integer = StringUtil.isInteger(nextBit);
                    if (integer == null) continue;
                    if (integer > 12) day2Vote++;
                    else {
                        day2Vote++;
                        month2Vote++;
                    }
                }
                // so if we have day2Votes - return MonthNumberic el
                if (day2Vote > 0) return BreakRule.Rule.MonthNumeric.name();
                else return BreakRule.Rule.DayNumeric.name();
            }
            // then we need to resolve the overlap because they numbers are less than 12 -

        }
        if (monthNumericVote > 1 && monthVote == 0 && monthNumericVote > dayNumericVote)
            return BreakRule.Rule.MonthNumeric.name();
        if (dayNumericVote > 0) return BreakRule.Rule.DayNumeric.name();
        if (monthVote > 0 && monthNumericVote == 0) return BreakRule.Rule.Month.name();

        return defaultRule;
    }

    private static HashSet<String> getMonthRules() {
        return new HashSet<String>();
    }

    static String[] monthString = new String[] { "JAN","FEB","MAR", "APR", "MAY", "JUN","JUL","AUG","SEP","OCT","NOV","DEC" };
	
	private static boolean lineStartsWithMonthString(String line) {
		if (line.length() <= 10) return false;
		String ll = line.substring(0, 3).toUpperCase();
		for (String mm : monthString) {
			if (ll.startsWith(mm)) return true;
		}
		return false;
	}

    static String[] dateSeps = new String[] { "/","-"," ","."};
    public static List<String> getMonthNumericStarters() {
        List<String> results = new ArrayList<String>();
        for (int i = 0; i < 13; i++) {
            for (String dateSep : dateSeps) {
                results.add(String.format(_02D + dateSep, i));
            }
        }
        return results;
    }
    public static List<String> getDayNumericStarters() {
        List<String> results = new ArrayList<String>();
        for (int i = 0; i < 31; i++) {
            for (String dateSep : dateSeps) {
                results.add(String.format(_02D + dateSep, i));
            }
        }
        return results;
    }

}
