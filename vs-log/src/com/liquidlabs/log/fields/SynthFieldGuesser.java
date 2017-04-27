package com.liquidlabs.log.fields;


import com.liquidlabs.log.fields.field.FieldI;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

public class SynthFieldGuesser implements SynthFieldGenerator {

    public static final String DEFAULT_DELIMETERS = "[\\s,&|:=;]";
    public static final String OVERRIDE_DELIMETERS = "[\\s,]";
    private final double percentMatch;
    private String[] tokens = {"=", ":"};
	private int synthLimit = Integer.getInteger("synth.limit", 5);;


    public SynthFieldGuesser(double percentMatch) {
        this.percentMatch = percentMatch;
    }

    public FieldSet guess(FieldSet fieldSet, String... data) {
        addSynthetics(fieldSet, findPossibleSynthetics(fieldSet, data), data);
        return fieldSet;
    }

    private Map<SynthField, Set<String>> findPossibleSynthetics(FieldSet fieldSet, String[] data) {
        final Map<SynthField, Set<String>> possibleSynths = new HashMap<SynthField, Set<String>>();
        for (FieldI field : fieldSet.fields()) {
            extractPossibleSyntheticsFromLine(fieldSet, possibleSynths, field, data);
        }
        return possibleSynths;
    }

    private void addSynthetics(FieldSet fieldSet, Map<SynthField, Set<String>> possibleSynths, String[] data) {
    	int added = 0;
        for (Map.Entry<SynthField, Set<String>> maybeSynth : possibleSynths.entrySet()) {
            if (maybeSynth.getValue().size() >= data.length * percentMatch) {
                SynthField synthField = maybeSynth.getKey();
                if (added < synthLimit)
                    fieldSet.addSynthField(synthField.syntheticName(), synthField.srcFieldName, synthField.expression(), "count", true, false);
                added ++;
            }
        }
    }

    private void extractPossibleSyntheticsFromLine(FieldSet fieldSet, Map<SynthField, Set<String>> possibleSynth, FieldI field, String[] data) {
        for (String line : data) {
            if (shouldConsiderThisLine(line)) {
                String fieldValue = fieldSet.getFieldValue(field.name(), fieldSet.getFields(line, -1, -1, -1));
                for (String token : tokens) {
                    extractForToken(possibleSynth, field, fieldValue, token);
                }
            }
        }
    }

    private boolean shouldConsiderThisLine(String line) {
        return !(line.trim().isEmpty() || line.startsWith("#new"));
    }

    private void extractForToken(Map<SynthField, Set<String>> possibleSynth, FieldI field, String fieldValue, String token) {
        String[] lineSplit = fieldValue.split(token);
        for (int i = 0; i < lineSplit.length - 1; i++) {
            String[] keySplit = lineSplit[i].split(DEFAULT_DELIMETERS);
            String name = keySplit[keySplit.length - 1].trim();
            if (canBeUsedAsKey(name)) {
                extractSynthetic(possibleSynth, field, token, lineSplit[i + 1], name);
            }
        }

    }

    private void extractSynthetic(Map<SynthField, Set<String>> possibleSynth, FieldI field, String token, String possibleValue, String name) {
        String[] valueSplit = possibleValue.split(delimetersBasedOnValue(possibleValue));
        String value = valueSplit[0].trim();
        addPossibleSynth(possibleSynth, field, name, token, value, valueSplit.length > 1 ? possibleValue.substring(value.length(), value.length() + 1) : "");
    }

    private String delimetersBasedOnValue(String possibleValue) {
        return useOverrideDelemiters(possibleValue) ? OVERRIDE_DELIMETERS : DEFAULT_DELIMETERS;
    }

    private boolean useOverrideDelemiters(String possibleValue) {
        return possibleValue.matches("(^(http)|^(tcp)|^(udp)|^(stcp:)).*(?i)");
    }

    private boolean canBeUsedAsKey(String possibleKey) {
        return !(useOverrideDelemiters(possibleKey) || possibleKey.matches("(\\d+)||(\\d+\\.\\d+)"));
    }

    private void addPossibleSynth(Map<SynthField, Set<String>> possibleSynth, FieldI field, String synthSuffix, String token, String value, String delim) {
        SynthField synthField = new SynthField(field.name(), synthSuffix, token, delim);
        if (possibleSynth.containsKey(synthField)) {
            possibleSynth.get(synthField).add(value);
        } else {
            HashSet<String> values = new HashSet<String>();
            values.add(value);
            possibleSynth.put(synthField, values);
        }
    }


    public class SynthField {
        private final String srcFieldName;
        private final String synthSuffix;
        private final String token;
        private String delim;

        public SynthField(String srcFieldName, String synthSuffix, String token, String delim) {
            this.srcFieldName = srcFieldName;
            this.synthSuffix = synthSuffix;
            this.token = token;
            this.delim = delim;
        }

        public String syntheticName() {
            return format("%s-%s", srcFieldName, synthSuffix);
        }

        @Override
        public boolean equals(Object o) {
            SynthField other = (SynthField) o;
            return srcFieldName.equals(other.srcFieldName) && synthSuffix.equals(other.synthSuffix) && delim.equals(other.delim);
        }

        @Override
        public int hashCode() {
            int hashCode = 31 * srcFieldName.hashCode();
            return hashCode + 31 * synthSuffix.hashCode();
        }

        public String expression() {
            if (delim.isEmpty()) {
                return format("substring,%s%s", synthSuffix, token);
            }
            return format("substring,%s%s,%s", synthSuffix, token, delim);

        }

    }

}

