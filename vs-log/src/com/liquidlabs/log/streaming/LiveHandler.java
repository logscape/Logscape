package com.liquidlabs.log.streaming;

import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.index.LogFile;

public interface LiveHandler {

	int handle(LogFile logfile, String path, long time, int line,
			String nextLine, String fileTag, FieldSet fieldSet, String[] fieldValues);

	boolean isExpired();

	String subscriber();

}
