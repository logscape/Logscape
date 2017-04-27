package com.liquidlabs.log.space;

import java.util.ArrayList;
import java.util.List;

import com.liquidlabs.log.fields.FieldSet;

public class LogConfiguration {

	private LogFilters filters = new LogFilters();
	private List<WatchDirectory> watching = new ArrayList<WatchDirectory>();
	private List<FieldSet> fieldSets = new ArrayList<FieldSet>();
	
	public LogConfiguration() {}
	
	public LogConfiguration(LogFilters filters, List<WatchDirectory> watching, List<FieldSet> fieldSets) {
		this.filters = filters;
		this.fieldSets = fieldSets;
		this.watching.addAll(watching);
	}
	
	public List<WatchDirectory> watching() {
		return watching;
	}
	
	public LogFilters filters() {
		return filters;
	}
	public List<FieldSet> fieldSets() {
		return fieldSets;
	}

    public boolean isPopulated() {
        return watching().size() != 0 && fieldSets.size() != 0;
    }
}
