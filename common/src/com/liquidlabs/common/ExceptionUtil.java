package com.liquidlabs.common;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtil {
	
	public static String stringFromStack(Throwable t, int range){
		StringWriter stringWriter = new StringWriter();
		t.printStackTrace(new PrintWriter(stringWriter));
		String string = stringWriter.toString();
		if (range > 0 && string.length() > 1024) string = string.substring(string.length()-1024, string.length()-1);
		return t.getMessage() + "\n" + string; 
	}


}
