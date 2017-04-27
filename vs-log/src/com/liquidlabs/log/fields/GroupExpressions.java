package com.liquidlabs.log.fields;

import com.liquidlabs.common.collection.Arrays;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class GroupExpressions {
    private List<FieldMatch> groups = new ArrayList<FieldMatch>();
    private final FieldDelimeter delimeter;

    public GroupExpressions(FieldDelimeter delimeter) {
        this.delimeter = delimeter;
    }

    public void add(FieldMatch group) {
        groups.add(group);
    }

    public FieldSet createFieldSet(String[] examples) {
        List<String> fieldNames = fixForFieldNames(examples[0]);
        FieldSet fieldSet = new FieldSet("name", examples, toRegex(), "*", 100);

        for (int i = 0; i < fieldCount(); i++) {
            fieldSet.addField(fieldName(i, fieldNames), "count()", true, true);
        }
        return fieldSet;
    }

    private List<String> fixForFieldNames(String example) {
        List<String> fieldNames  = extractFieldNamesFrom(example);

        if (!fieldNames.isEmpty() && fieldCount() > fieldNames.size()) {
            reduceTo(fieldNames.size());
        }
        return fieldNames;
    }

    private String fieldName(int i, List<String> fieldNames) {
        return fieldNames.isEmpty() ? group(i).guessedName() : fieldNames.get(i);
    }

    private List<String> extractFieldNamesFrom(String firstLine) {
        if (firstLine.startsWith("#new ")) {
            return Arrays.asList(firstLine.substring("#new ".length(), firstLine.length()).split("\\s+"));
        }
        return Collections.emptyList();
    }


    public int fieldCount() {
        return groups.size();
    }

    public GroupExpressions mergeWith(GroupExpressions groupExpression) {
        if (!sameNumberOfFields(groupExpression)) {
            reduceBothToMaxFields(groupExpression);
        }

        List<FieldMatch> newGroups = mergeMatches(groupExpression);
        int lastStar = findLastIndex(newGroups);
        Collections.reverse(newGroups);

        return buildResult(newGroups, lastStar);
    }

    @Override
    public int hashCode() {
        return getClass().getSimpleName().hashCode() + groups.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof GroupExpressions && this.groups.equals(((GroupExpressions) o).groups);
    }


    private String toRegex() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < groups.size(); i++) {
            builder.append(groups.get(i).regex());
            if (i < groups.size() - 1) {
                builder.append(delimeter.delim());
            }
        }
        return builder.toString();

    }

    private GroupExpressions buildResult(List<FieldMatch> newGroups, int lastStar) {
        GroupExpressions result = new GroupExpressions(delimeter);
        for (int i = 0; i < lastStar; i++) {
            result.add(newGroups.get(i));
        }
        return result;
    }

    private int findLastIndex(List<FieldMatch> newGroups) {
        int index = newGroups.lastIndexOf(AutoFieldGuesser.TheRest);
        if (index == -1) {
            return newGroups.size();
        }

        return newGroups.size() - index;
    }

    private List<FieldMatch> mergeMatches(GroupExpressions groupExpression) {
        List<FieldMatch> newGroups = new ArrayList<FieldMatch>();
        for (int i = maxFields(groupExpression) - 1; i >= 0; i--) {
            FieldMatch mergeResult = groups.get(i).mergeWith(groupExpression.groups.get(i));
            newGroups.add(mergeResult);
        }
        return newGroups;
    }


    private void reduceBothToMaxFields(GroupExpressions groupExpression) {
        int maxFields = maxFields(groupExpression);
        this.reduceTo(maxFields);
        groupExpression.reduceTo(maxFields);
    }

    private void reduceTo(int maxFields) {
        if (maxFields < groups.size()) {
            groups = new ArrayList<FieldMatch>(groups.subList(0, maxFields - 1));
            groups.add(AutoFieldGuesser.TheRest);
        }
    }

    private boolean sameNumberOfFields(GroupExpressions groupExpression) {
        return this.fieldCount() == groupExpression.fieldCount();
    }


    private int maxFields(GroupExpressions other) {
        return this.fieldCount() > other.fieldCount() ? other.fieldCount() : this.fieldCount();
    }



    private FieldMatch group(int i) {
        return groups.get(i);
    }
}
