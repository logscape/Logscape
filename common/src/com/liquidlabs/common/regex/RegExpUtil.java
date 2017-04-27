package com.liquidlabs.common.regex;

import jregex.Matcher;
import jregex.REFlags;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.joda.time.DateTimeUtils;

import java.text.DecimalFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Yes it sucks but it makes the code cleaner
 *
 * the following has some benchmarks
 * http://www.ibm.com/developerworks/web/library/wa-regexp.html
 * http://www.tusker.org/regex/regex_benchmark.html
 */
public class RegExpUtil {

    private static final int MIN_ONE = 1;
    //	private static final String REGEXP_EOL_CHAR = "\\s+";
    private static final String REGEXP_EOL_CHAR = "\\s";
    private static final String NEW_LINE_N = "\n";
    private static final String NEWLINE_R = "\r";

    public static boolean isMatch(String itemToCheck, String[] patterns) {
        // process excludes first!
        for (String pattern : patterns) {
            if (pattern.startsWith("!")) {
                String aPattern = pattern.substring(1);
                if (itemToCheck.contains(aPattern) || itemToCheck.matches(aPattern)) return false;
            }
        }

        for (String pattern : patterns) {
            if (pattern.startsWith("!")) continue;
            if (itemToCheck.matches(pattern)) return true;
        }
        return false;

    }

    // lazy version
    public static com.liquidlabs.common.regex.MatchResult matchesJava(String regExp, String line)  {
        boolean multiline = isMultiline(line);
//			Pattern pattern = Pattern.compile(regExp, Pattern.DOTALL | Pattern.MULTILINE);
//			Pattern pattern = Pattern.compile(regExp, Pattern.DOTALL | Pattern.MULTILINE);
        Pattern pattern = Pattern.compile(regExp, Pattern.DOTALL | Pattern.MULTILINE);
        java.util.regex.Matcher matcher = pattern.matcher(line);
//			compile.matches(regExp, line);
//			jregex.Pattern patternMatch = new jregex.Pattern(regExp, REFlags.MULTILINE);
        return matches(pattern, matcher, line, multiline);
    }
    // lazy version
    public static com.liquidlabs.common.regex.MatchResult matchesJava(Pattern pattern, String line)  {
        boolean multiline = isMultiline(line);
//			Pattern pattern = Pattern.compile(regExp, Pattern.DOTALL | Pattern.MULTILINE);
//		Pattern pattern = Pattern.compile(regExp, Pattern.DOTALL | Pattern.MULTILINE);
        java.util.regex.Matcher matcher = pattern.matcher(line);
//			compile.matches(regExp, line);
//			jregex.Pattern patternMatch = new jregex.Pattern(regExp, REFlags.MULTILINE);
        return matches(pattern, matcher, line, multiline);
    }
    // lazy version
    public static com.liquidlabs.common.regex.MatchResult matchesORO(String regExp, String line)  {
        Perl5Compiler oCompiler = new Perl5Compiler();
        try {
//			org.apache.oro.text.regex.Pattern oPattern = oCompiler.compile(regExp, Perl5Compiler.DEFAULT_MASK | Perl5Compiler.SINGLELINE_MASK
//					| Perl5Compiler.MULTILINE_MASK | Perl5Compiler.READ_ONLY_MASK);
            org.apache.oro.text.regex.Pattern oPattern = oCompiler.compile(regExp, Perl5Compiler.DEFAULT_MASK
                    | Perl5Compiler.SINGLELINE_MASK
//					| Perl5Compiler.MULTILINE_MASK | Perl5Compiler.READ_ONLY_MASK
            );
            Perl5Matcher oMatcher = new Perl5Matcher();
            boolean matches = oMatcher.matches(line, oPattern);

            MatchResult matchResult = new MatchResult(oMatcher, matches, false);
            return matchResult;
        } catch (MalformedPatternException e) {
            throw new RuntimeException("Bad Expression:" + regExp, e);
        }
    }

    private static MatchResult matches(Pattern pattern, java.util.regex.Matcher matcher, String line, boolean multiline) {
        return new com.liquidlabs.common.regex.MatchResult(matcher, multiline);
    }
    public static com.liquidlabs.common.regex.MatchResult matches(String regExp, String line)  {
        if (regExp.equals("*")) return new MatchResult(new String[] { line });
        return matchesJREx(regExp, line);
//		return matchesJava(regExp, line);
//		return matchesORO(regExp, line);
    }
    public static com.liquidlabs.common.regex.MatchResult matchesJREx(String regExp, String line)  {
        boolean multiline = isMultiline(line);
        jregex.Pattern patternMatch = new jregex.Pattern(regExp, REFlags.MULTILINE | REFlags.DOTALL);
        return matchesJRExp(patternMatch, patternMatch.matcher(), line, multiline);
    }

    public static com.liquidlabs.common.regex.MatchResult matchesJRExp(jregex.Pattern patternMatch, Matcher matcher2, String string, boolean isMultiline) {
        if (!matcher2.matches(string)) return new com.liquidlabs.common.regex.MatchResult();
        return new com.liquidlabs.common.regex.MatchResult(matcher2, isMultiline);
    }

    public static final boolean isMultiline(final String line) {
        int indexOfNL = line.indexOf(NEW_LINE_N);
        if (indexOfNL == line.length() - 1) return false;
        return (indexOfNL != -1) || line.indexOf(NEWLINE_R) != -MIN_ONE;
    }

    public static String[] getCommaStringAsRegExp(String stringToSplit, boolean isWindows) {
        String[] split = stringToSplit.split(",");
        String[] patterns = new String[split.length];
        int pos = 0;
        for (String pattern : split) {
            pattern = pattern.trim();
            String item = FileMaskAdapter.adapt(pattern, isWindows);
            if (item.startsWith("!")) {
                item =  "!" + SimpleQueryConvertor.convertSimpleToRegExp(item.substring(1));
            }
            else item = SimpleQueryConvertor.convertSimpleToRegExp(item);
            patterns[pos++] = item;
        }
        return patterns;
    }

    public String getLastWordInToken(String string) {
        MatchResult matches = RegExpUtil.matches(".*?(\\w+)", string);
        if (!matches.isMatch()) return null;
        String group = matches.group(matches.groups()-1);
        if (group.contains("_") && !group.endsWith("_")) return group.substring(group.lastIndexOf("_")+1, group.length());
        return group;
    }




    public static String testJRegExp(String regExp, final String text) {

        try {

            StringSplitEval stringSplitEval = null;
            if (regExp.startsWith("split(")) {
                stringSplitEval = new StringSplitEval(regExp);
            }
            else if (SimpleQueryConvertor.isSimpleLogFiler(regExp)) {
                regExp = SimpleQueryConvertor.convertSimpleToRegExp(regExp);
            }

            MatchResult matches = RegExpUtil.matches(regExp, text);
            if (stringSplitEval != null) matches = stringSplitEval.evaluate(text);

            if (!matches.match) return String.format("TEST:\t%s\nREGEXP:\t%s\nMSG:\tNo match found", text, regExp);

            StringBuilder result = new StringBuilder();
            String title = "<font size='12'><b>Expression Test Results</b></font>";
            result.append("<b>Expression</b> :").append(StringEscapeUtils.escapeXml(regExp)).append("\n");
            result.append("<b>Group count</b>:").append(matches.groups()).append("\n");
            for (int i = 0; i < matches.groups(); i++) {
                result.append("<b>").append(i).append(")</b>").append(" ");
                result.append(StringEscapeUtils.escapeXml(matches.group(i))).append(",\n");
            }

            final AtomicInteger count = new AtomicInteger();
            final jregex.Pattern patternMatch = stringSplitEval != null ? null : new jregex.Pattern(regExp, REFlags.MULTILINE | REFlags.DOTALL);
            final StringSplitEval ssEvl = stringSplitEval;
            final AtomicBoolean isDone = new AtomicBoolean(false);

            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    while (!isDone.get()) {
                        if (ssEvl != null) ssEvl.evaluateGroups(text);
                        else {
                            Matcher matcher = patternMatch.matcher(text);
                            if (matcher.matches()) matcher.groups();
                        }
                        count.incrementAndGet();
                    }

                }
            };
            Future<?> submit = Executors.newSingleThreadExecutor().submit(runnable);
            long waitFor = 1000;
            try {
                submit.get(waitFor, TimeUnit.MILLISECONDS);

            } catch (Throwable t) { }
            isDone.set(true);

            long elapsed = waitFor;

            String outcome = "";
            if (count.get() < 100 * 1000) outcome += ("<b>*** WARNING, slow performance ***</b>");
            DecimalFormat formatter = new DecimalFormat("#,###");
            outcome += String.format("\n<i>Expression Performance: %s</i> (EventsPerSec)\n", formatter.format(count.get()));

            return title + "\n" + outcome + "\n" + result.toString();
        } catch (Throwable e) {
            return "Failed to process[" + regExp + "] ex:" + e.getMessage();
        }
    }
    public static String testJavaRegExp(String regExp, String text) {

        try {
            if (SimpleQueryConvertor.isSimpleLogFiler(regExp)) {
                regExp = SimpleQueryConvertor.convertSimpleToRegExp(regExp);
            }

            MatchResult matches = RegExpUtil.matchesJava(regExp, text);

            if (!matches.match) return String.format("TEST:\t%s\nREGEXP:\t%s\nMSG:\tNo match found", text, regExp);

            StringBuilder result = new StringBuilder();
            result.append("Expression :").append(regExp).append("\n");
            result.append("Group count:").append(matches.groups()).append("\n");
            for (int i = 0; i < matches.groups(); i++) {
                result.append(i).append(")").append(" ");
                result.append(matches.group(i)).append(",\n");
            }

            int limit = 500;
            long start = DateTimeUtils.currentTimeMillis();
            for (int i = 0; i < limit; i++) {
                RegExpUtil.matches(regExp, text);
            }
            long end = DateTimeUtils.currentTimeMillis();

            long elapsed = end - start;
            if (elapsed > 30) result.append("*** Warning, elapsed time > 30ms ***");
            result.append(String.format("\nPerformance: %d count iteration, elapsed:%dms\n", limit, elapsed));

            return result.toString();
        } catch (Throwable e) {
            return "Failed to process[" + regExp + "] ex:" + e.getMessage();
        }
    }

    public static boolean isMultilinePattern(String pattern) {
        return pattern.contains(REGEXP_EOL_CHAR);
    }
    public static String getNextWordAfter(String before, String all) {
        if (all == null) return "";
        try {
            all = all.replaceAll("\\n", "");
            all = all.replaceAll("\\r", "");
            String[] split = all.split(before);
            if (split.length == 0) return null;
            MatchResult mr = matchesJava("(\\w+).*", split[1]);
            if (mr.isMatch()) return mr.getGroup(1);
            return null;
        } catch (Throwable t) {
            System.err.println("INPUT:" + before + " \nALL:" +  all);
            return null;
        }
    }

}
