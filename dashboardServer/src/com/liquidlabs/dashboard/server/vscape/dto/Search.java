package com.liquidlabs.dashboard.server.vscape.dto;

import java.util.ArrayList;
import java.util.List;

public class Search {
	public String name = "";
	public String title = "";
	public String owner = "";
	public List<String> queries = new ArrayList<String>();
	public String logFileFilter = "";
	public List<Integer> palette = new ArrayList<Integer>();
	public Integer replayPeriod;
	public int lastRecordCount = 0;
	public String variables = "";
	public boolean simpleMode = false;
	
	public Search(){};
	
	public Search(String name, String title, String owner, List<String> queries, String logFileFilter, List<Integer> palette, int replayPeriod, String variables) {
		this.name = name;
		this.title = title;
		this.owner = owner;
		this.queries = queries;
		this.logFileFilter = logFileFilter;
		this.palette = palette;
		this.replayPeriod = replayPeriod;
		if (variables != null) this.variables = variables;
	}
	
	public Search(com.liquidlabs.log.space.Search report) {
		this(report.name, report.title, report.owner, report.patternFilter, report.logFileFilter, report.palette, report.replayPeriod, report.variables);
	}

	public boolean isColoured() { 
		return palette.get(0) != 0; 
	};
	
}