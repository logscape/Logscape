package com.liquidlabs.log.fields;


public interface FieldGenerator {
    FieldSet guess(String... examples);
}
