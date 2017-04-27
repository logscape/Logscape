package com.liquidlabs.log.space;

import java.util.Arrays;

import com.liquidlabs.common.regex.SimpleQueryConvertor;
import com.liquidlabs.orm.Id;

public class LogFilters {
	public static String key = "LiveFilters";

	@Id
	public String id = key;
	public String [] includes = {};
	public String [] excludes = {};
	
	public LogFilters() {}
	
	public LogFilters(String [] includes, String [] excludes) {
		this.includes = includes == null ? new String[0] : includes;
		this.excludes = excludes == null ? new String[0] : excludes;
	}
	private String[] includesRegexp;
	private String[] excludesRegexp;
	
	public String[] includes() {
		if (includesRegexp == null) includesRegexp = new SimpleQueryConvertor().convertArray(includes);
		return includesRegexp;
	}

	public String[] excludes() {
		if (excludesRegexp == null) excludesRegexp = new SimpleQueryConvertor().convertArray(excludes);
		return excludesRegexp;
	}
	
	public String toString() {
		return getClass().getSimpleName() + " id:" + id + " i:" + Arrays.toString(includes) + " x:" + Arrays.toString(excludes);
	}
	
}
