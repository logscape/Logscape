package com.liquidlabs.log.fields;

public interface SynthFieldGenerator {
    FieldSet guess(FieldSet fieldSet, String... data);
}
