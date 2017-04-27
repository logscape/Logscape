package com.liquidlabs.log.fields;

import com.liquidlabs.common.collection.Arrays;

import java.util.List;

/**
 * Created by neil on 16/07/2015.
 */
public class CsvFieldGenerator implements FieldGenerator {
    @Override
    public FieldSet guess(String... examples) {
        String[] fieldNames = examples[0].split(",");
        FieldSet fieldSet = new FieldSet("split(,)", fieldNames);
        fieldSet.example = examples;
        return fieldSet;
    }
}
