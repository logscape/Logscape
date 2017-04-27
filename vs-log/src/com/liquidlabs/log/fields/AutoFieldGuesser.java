package com.liquidlabs.log.fields;


import com.liquidlabs.common.collection.Arrays;
import org.apache.log4j.Logger;

import java.util.*;

import static java.lang.String.format;


public class AutoFieldGuesser implements FieldGenerator {

    private static final Logger LOGGER = Logger.getLogger(AutoFieldGuesser.class);
    private static final String NUMERIC = "(\\d+)";
    private static final String WORD = "(\\w+)";
    private static final String IPV4 = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})";
    private static final String STAR = "(.*)";

    public static final FieldMatch TheRest = new FieldMatch("Data", STAR);

    private final Collection<ExpressionMatcher> matchers = new ArrayList<ExpressionMatcher>();
    private final ExpressionMatcher defaultMatcher;
    private final FieldDelimeter delimeter;


    public AutoFieldGuesser() {
        this(FieldDelimeter.Space);
    }


    public AutoFieldGuesser(FieldDelimeter delimeter) {
        this.delimeter = delimeter;
        matchers.add(new FieldMatcher("Date", dates()));
        matchers.add(new FieldMatcher("Time", times()));
        matchers.add(new FieldMatcher("IpAddress", listWith(IPV4)));
        matchers.add(new FieldMatcher("DateTime", dateTimes()));
        matchers.add(new DelegatingMatcher(new FieldMatcher("WebAddress",
                webAddresses()), delimeter.defaultMatch()));
        matchers.add(new DelegatingMatcher(new FieldMatcher("Level", listWith("(FINE|FINER|DEBUG|INFO|ERROR|WARN|FATAL|TRACE)")), WORD));
        matchers.add(new FieldMatcher("Number", listWith(NUMERIC)));
        matchers.add(new FieldMatcher("Word", listWith(WORD)));
        defaultMatcher = new FieldMatcher("Field", listWith(delimeter.defaultMatch()));
    }


    @Override
    public FieldSet guess(String... examples) {
        dumpExamples(examples);
        if (examples.length == 0) {
            return new FieldSet();
        }

        String[] withoutHeader = removeHeader(examples);
        Set<GroupExpressions> allGroups = buildExpressions(withoutHeader);

        if (allGroups.size() == 1) {
            return buildFieldSet(allGroups.iterator().next(), examples);
        }

        return reduceToSingleMatch(allGroups, examples);
    }

    private void dumpExamples(String[] examples) {
        if (LOGGER.isDebugEnabled()) {
            for (int i = 0; i < examples.length; i++) {
                LOGGER.debug(format("Example %d = %s", i, examples[i]));
            }
        }
    }

    private String[] removeHeader(String[] examples) {
        if (examples[0].startsWith("#new")) {
            String[] cleaned = new String[examples.length - 1];
            System.arraycopy(examples, 1, cleaned, 0, examples.length - 1);
            return cleaned;
        }
        return examples;
    }

    private FieldSet reduceToSingleMatch(Set<GroupExpressions> allGroups, String[] examples) {
        GroupExpressions[] groupExpressions = allGroups.toArray(new GroupExpressions[0]);
        GroupExpressions result = groupExpressions[0];

        for (int i = 1; i < groupExpressions.length; i++) {
            result = result.mergeWith(groupExpressions[i]);
        }

        return buildFieldSet(result, examples);
    }

    private FieldSet buildFieldSet(GroupExpressions result, String[] examples) {
        return result.createFieldSet(examples);
    }

    private Set<GroupExpressions> buildExpressions(String[] examples) {
        Set<GroupExpressions> allGroups = new HashSet<GroupExpressions>();
        for (String example : examples) {
            extractNext(allGroups, example);
        }
        return allGroups;
    }


    private void extractNext(Set<GroupExpressions> allGroups, String example) {
        if (!example.trim().isEmpty()) {
            String[] parts = example.trim().split(delimeter.delim());
            GroupExpressions groups = new GroupExpressions(delimeter);
            for (String part : parts) {
                groups.add(nextGroup(part));
            }
            allGroups.add(groups);
            resetMatchers();
        }
    }

    private void resetMatchers() {
        for (ExpressionMatcher matcher : matchers) {
            matcher.resetCount();
        }
        defaultMatcher.resetCount();
    }

    private FieldMatch nextGroup(String part) {
        for (ExpressionMatcher matcher : matchers) {
            if (matcher.isMatch(part)) {
                return matcher.createFieldMatch(part);
            }
        }

        return defaultMatcher.createFieldMatch(part);

    }

    private List<String> times() {
        return listWith("(\\d{1,2}:\\d{1,2}:\\d{1,2})", "(\\d{1,2}:\\d{1,2}:\\d{1,2}[\\.,]\\d{3})");
    }

    private List<String> dateTimes() {
        String d1 = "\\d{4}-\\d{1,2}-\\d{1,2}";
        List<String> dateTimes = new ArrayList<String>();

        List<String> times = times();
        for (String time : times) {
            dateTimes.add(format("(%sT%s)", d1, removeBrackets(time)));
        }

        List<String> dates = dates();
        for (String date : dates) {
            for (String time : times) {
                dateTimes.add(format("(%s[\\s]+%s)", removeBrackets(date), removeBrackets(time)));
            }
        }
        return dateTimes;


    }

    private List<String> webAddresses() {
        return listWith(
                "(http://www\\.[^\\.]+\\.\\w+)", "(https://www\\.[^\\.]+\\.\\w+)",
                "(http://[^\\.]\\.[^\\.].[\\w+])", "(https://[^\\.]\\.[^\\.].[\\w+])",
                "(http://[^\\s]+)", "(https://[^\\s]+)",
                "(www\\.[^\\.]+\\.\\w+)", "(www\\.[^\\.]+\\.[^\\s]+)");
    }


    private String removeBrackets(String expression) {
        return expression.substring(1, expression.length() - 1);
    }

    private List<String> dates() {
        return listWith("(\\d\\d\\d\\d[-/\\.]\\d\\d[-/\\.]\\d\\d)",
                "(\\d\\d[-/\\.]\\d\\d[-/\\.]\\d\\d\\d\\d)",
                "(\\d{1,2} [a-zA-Z]{3} \\d{4})",
                "(\\d{1,2} [a-zA-Z]{3} \\d{2})",
                "(\\d{1,2} [a-zA-Z]{3})",
                "(\\d{2}-[a-zA-Z]{3}-\\d{4})",
                "(\\d{2}-[a-zA-Z]{3}-\\d{2})");
    }


    private List<String> listWith(String... strings) {
        return Arrays.asList(strings);
    }


}
