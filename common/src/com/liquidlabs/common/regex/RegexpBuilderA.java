package com.liquidlabs.common.regex;

import com.liquidlabs.common.collection.Arrays;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RegexpBuilderA {

    private final List<String> delimiters = Arrays.asList(" ", "\t");
    private final List<String> delimiters2 = Arrays.asList(":", "=");
    HashMap<String, String> library = new HashMap<String, String>();
    ArrayList<String> order = new ArrayList<String>();

    public RegexpBuilderA() {
        add("RE_Integer", "\\d+");
        add("RE_Decimal", "\\d+\\.\\d+");
        add("RE_Email", "[A-Za-z0-9\\-\\.\\_]+@[A-Za-z0-9\\-\\_]+\\.[A-Za-z]+");
        add("RE_IpAddress", "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+");
        add("RE_Date1", "[0-9]+-[0-9]+-[0-9]+");
        add("RE_Date2", "[0-9]+-[A-Za-z]+-[0-9]+");
        add("RE_Date3", "[0-9]+/[0-9]+/[0-9]+");
        add("RE_Time2", "[0-9]+\\:[0-9]+\\:[0-9]+");
        add("RE_Time1", "[0-9]+\\:[0-9]+\\.[0-9]+");
        add("RE_Time3", "[0-9]+\\:[0-9]+\\.[0-9]+\\,[0-9]+");
        add("RE_Time4", "[0-9]+\\:[0-9]+\\:[0-9]+\\,[0-9]+");
        add("RE_Time5", "[0-9]+\\:[0-9]+\\:[0-9]+\\.[0-9]+");
        add("RE_UCaseWord", "[A-Z\\_\\-]+");
        add("RE_Word", "\\w+");
//		add("RE_WordWithDashes", "[A-Z\\_\\-]+");
        add("RE_PackageWord", "[A-Za-z\\.]+");
        add("RE_HttpAddress", "http:\\/\\/[A-Za-z\\.\\-]+\\/[\\/A-Za-z0-9]+");
    }

    public String getTagString(String string) {
        return getTagString(string, null);
    }


    public String getTagString(String string, String selectedText) {
        boolean selected = selectedText != null && selectedText.length() > 0;
        if (selected) string = string.replace(selectedText, "");
        List<String> splitString = this.split(string);

        StringBuilder stringBuilder = createPattern(selected, splitString);

        if(stringBuilder.toString().trim().equals(string)) {
             if (splitString.size() == 1) {
                return createPattern(selected, splitCsv(string)).toString().trim();
             }
        }

        return stringBuilder.toString().trim();
    }

    private StringBuilder createPattern(boolean selected, List<String> splitString) {
        StringBuilder stringBuilder = new StringBuilder();
        if (selected) stringBuilder.append(".*");

        for (String item : splitString) {
            stringBuilder.append(getTagForItem(item)).append(" ");

        }
        return stringBuilder;
    }

    private List<String> splitCsv(String string) {
        return new CsvParser(string).parse();
    }

    public String extractToRegexp(String regExpInterp) {
        if (regExpInterp.indexOf("RE_") == -1) return regExpInterp;
//		StringBuilder result = new StringBuilder();
        for (String libraryKey : order) {
            if (regExpInterp.contains(libraryKey)) {
                String replacement = library.get(libraryKey);
                replacement = replacement.replaceAll("\\\\", "\\\\\\\\");
                regExpInterp = regExpInterp.replaceAll(libraryKey, replacement);
            }
        }
        // now drop in the spaces
        return regExpInterp.replaceAll("\\s+", "\\\\s+");
    }

    private String getTagForItem(String item) {
        boolean grouping = isGrouped(item);
        if (grouping) item = item.substring(1, item.length() - 1);
        for (String regexpTag : this.order) {
            if (isMatch(library.get(regexpTag), item)) {
                return grouping ? "(" + regexpTag + ")" : regexpTag;
            }
        }
        for (String interDelim : delimiters2) {
            if (item.contains(interDelim)) {
                String[] split = item.split(interDelim);
                if (split.length == 2) {
                    item = split[0] + interDelim + getTagForItem(split[1]);
                }
            }
        }
//		String replaceAll = item.replaceAll("\\(", "\\\\(").replaceAll("\\)", "\\\\)").replaceAll("\\[", "\\\\[").replaceAll("\\]", "\\\\]");//.replaceAll("\\-", "\\\\-");
        String replaceAll = item.replaceAll("\\[", "\\\\[").replaceAll("\\]", "\\\\]");//.replaceAll("\\-", "\\\\-");
        return grouping ? "(" + replaceAll + ")" : replaceAll;
    }

    /**
     * Split into space,tab, commas
     *
     * @param line
     * @return
     */
    public List<String> split(String line) {
        List<String> stringList = Arrays.recursiveSplit(Arrays.asList(line), new ArrayList<String>(delimiters));

        return stringList;
    }


    public boolean isMatch(String expression, String string) {
        return RegExpUtil.matches(expression, string).match;
    }

    private void add(String tag, String expression) {
        order.add(tag);
        library.put(tag, expression);
    }

    public String removeGroups(String string) {
        List<String> split = this.split(string);
        StringBuilder result = new StringBuilder();
        int pos = 0;
        for (String string2 : split) {
            if (string2.startsWith("(") && string2.endsWith(")")) string2 = string2.substring(1, string2.length() - 1);
            result.append(string2);
            if (pos++ < split.size() - 1) result.append(" ");
        }
        return result.toString();
    }

    public List<String> extractToRegexp(List<String> tagList) {
        ArrayList<String> result = new ArrayList<String>();
        for (String tagString : tagList) {
            boolean hasCmd = tagString.contains(" | ");
            String cmd = hasCmd ? tagString.substring(tagString.indexOf(" |"), tagString.length()) : "";
            String regexp = hasCmd ? tagString.substring(0, tagString.indexOf(" |")) : tagString;

            result.add(extractToRegexp(regexp) + cmd);
        }
        return result;
    }

    private boolean isGrouped(String word) {
        return word.startsWith("(") && word.endsWith(")");
    }

    public String getSplitRegExp(String textToInterp, String selectedText) {
        String[] splitSpaces = textToInterp.split("\\s");
        String[] splitCommas = textToInterp.split(",");
        boolean isSpace = splitSpaces.length > splitCommas.length;
        String group = isSpace ? "(.*)" : "([^,]+)";
        String betweenCroup = isSpace ? "\\s+" : ",";

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < splitSpaces.length; i++) {
            String cVal = splitSpaces[i];
            if (cVal.equals(selectedText)) {
                result.append(group).append(betweenCroup).append("(.*)");
                return result.toString();
            }
            if (i < splitSpaces.length - 1) {
                result.append(group).append(betweenCroup);
            } else {
                result.append(group);
            }
        }
        return result.toString();
	}


}
