package com.liquidlabs.log.alert.correlate;

import com.liquidlabs.common.collection.Arrays;

import java.util.List;


public class SequenceRule implements Rule {
    String [] sequence;
    int index =0;


    public SequenceRule(String...sequence) {
		this.sequence = sequence;

    }

	public boolean evaluate(Event event) {
        Object fieldValue = event.getFieldValue();
        if (fieldValue == null) throw new RuntimeException("Failed to extract FieldValue for:" + event);
		return fieldValue.equals(sequence[index++]);
    }

    public boolean complete() {
        return index == sequence.length;
    }

    public Rule copy() {
        return new SequenceRule(sequence);
    }

	public boolean evaluate(List<Event> events) {
		return false;
	}

    public String describe(List<Event> events) {
        return "Sequence:" + Arrays.toString(sequence);
    }

}
