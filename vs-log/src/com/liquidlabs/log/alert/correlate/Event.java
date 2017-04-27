package com.liquidlabs.log.alert.correlate;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.ReplayEvent;


public class Event {

    private String fieldValue;
	private final String key;
	private ReplayEvent event;
	private final String fieldName;
	private final String keyField;
	private final String fieldSetId;

	public Event(ReplayEvent event, FieldSet fieldSet, String fieldName, String keyField) {
		this.fieldSetId = fieldSet.getId();
		this.fieldName = fieldName;
		this.keyField = keyField;
		this.fieldValue = fieldSet.getFieldValue(fieldName,  event.getFieldValues(fieldSet));
		if (this.fieldValue == null) throw new RuntimeException("Null Field Value:" + fieldName);
		this.key = fieldSet.getFieldValue(keyField,  event.getFieldValues(fieldSet));
		this.event = event;
	}

	public String getFieldValue() {
		return fieldValue;
	}

    public boolean occurredBeforeOrAt(long time) {
        return time() <= time;
    }
    
    public boolean occuredAtOrAfter(long time) {
    	return time() >= time;
    }

    public long time() {
        return event.getTime();
    }
    
    public String getKey() {
		return key;
	}
    
    @Override
    public boolean equals(Object obj) {
    	return time() == ((Event)obj).time();
    }
    
    @Override
    public String toString() {
    	return String.format("EventTime:%s FieldSetId:%s KeyFieldName:%s FieldName:%s Key:%s Value:%s Replay:%s", DateUtil.shortDateTimeFormat3.print(time()), fieldSetId, keyField, fieldName, key, fieldValue, this.event);
    }

	public String getRawData() {
		return event.getRawData();
	}

	public ReplayEvent getReplay() {
		return event;
	}

}
