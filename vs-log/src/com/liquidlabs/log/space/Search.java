package com.liquidlabs.log.space;

import com.liquidlabs.orm.Id;

import java.util.ArrayList;
import java.util.List;

/**
 * @author neil
 */
public class Search {
	
	@Id
	public String name = "";
	
	public String title = "";
	public String owner = "";
	public List<String> patternFilter = new ArrayList<String>();
	public String logFileFilter = "";
	public List<Integer> palette = new ArrayList<Integer>();
	public Integer replayPeriod;
	public int lastRecordCount = 0;
	public String variables = "";
	
	@Deprecated
	public boolean simpleMode = false;
	
	public Search(){};
	
//	public Search(String name, String owner, String patternFilter, String logFileFilter, int palette, int searchPeriod, String variables) {
//		this.name = name;
//		this.owner = owner;
//		this.patternFilter = Arrays.asList( patternFilter );
//		this.logFileFilter = logFileFilter;
//		this.palette = Arrays.asList( palette );
//		this.replayPeriod = searchPeriod;
//		if (variables != null) this.variables = variables;
//		
//	}
	public Search(String name, String owner, List<String> patternFilter, String logFileFilter, List<Integer> palette, int searchPeriod, String variables) {
		this.name = name.trim();
		this.owner = owner.trim();
		this.patternFilter = patternFilter;
		this.logFileFilter = logFileFilter;
		this.palette = palette;
		this.replayPeriod = searchPeriod;
		if (variables != null) this.variables = variables;
	}
	
	public boolean isColoured() { 
		return palette.get(0) != 0; 
	}

}