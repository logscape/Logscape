package com.liquidlabs.common.regex;


public class FileMaskAdapter {
	
	private static final char DOT = '.';
	private static final char STAR = '*';
	private static final char BACKSLASH = '\\';





	public static String adapt(String input, boolean isWindows) {
        if (input == null) return "";
		if (input.length() > 2 && input.charAt(1) == ':') isWindows = true;
		StringBuilder result = new StringBuilder();
        String[] inputs = input.split(",");
        for(String val : inputs) {
		    String fixDirSeperatorsForPlatform = fixDirSeperatorsForPlatform(handleWildcard(handleDot(val)), isWindows);
		    if (isWindows) {
			    fixDirSeperatorsForPlatform = fixForWindows(fixDirSeperatorsForPlatform);
		    }
		    result.append(fixDirSeperatorsForPlatform).append(",");
        }
        return result.substring(0, result.length()-1);

	}

	private static String fixForWindows(String input) {
		StringBuilder result = new StringBuilder();
		byte[] bytes = input.getBytes();
		for (int i = 0; i < bytes.length; i++) {
			byte b = bytes[i];
			if (i < bytes.length -1 && b == '\\' && (bytes[i+1] != '\\' && bytes[i+1] != '.')) {
				result.append("\\\\");				
			} else {
				result.append((char) b);
			}
			
		}
		return result.toString();
	}


	final public static String fixDirSeperatorsForPlatform(String filter, boolean isWindows) {
			if (isWindows){
				return  filter.replaceAll("/", "\\\\");
			} else {
				return replaceBackSlash(filter);
			}
	}
	
	public static String handleWildcard(String expression) {
		StringBuilder result = new StringBuilder();
		byte[] bytes = expression.getBytes();
		
		for (int i = 0; i < bytes.length; i++) {
			byte b = bytes[i];
			if (b == STAR && (i == 0 || (i > 0) && bytes[i-1] != DOT)) {
				result.append(".*");
			} else {
				result.append((char) b);
			}
		}
		return result.toString();

	}

	public static String handleDot(String expression) {
		StringBuilder result = new StringBuilder();
		char[] chars = expression.toCharArray();
		
		for (int i = 0; i < chars.length; i++) {
			char b = chars[i];
			if (b == '.' && (i > 0) &&
					chars[i-1] != DOT &&
					chars[i-1] != BACKSLASH) {
				if (i < chars.length -1 && chars[i+1] != STAR || i == chars.length -1){
                    result.append("\\.");
                }
				else {
                    result.append(".");
                }
			} else if (b== DOT && i == 0 && chars[i+1] != STAR) {
				result.append(".*");
			} else {
				result.append(b);
			}
		}
		return result.toString();
	}
	
	static public String replaceBackSlash(String path) {
		
		
		StringBuilder result = new StringBuilder();
		
		byte[] bytes = path.getBytes();
		int pos = 0;
		for (int b : bytes) {
			int nextByte  = -1;
			if (pos < bytes.length-1) nextByte = bytes[pos+1];
			else nextByte = -1;
			
			if (b == BACKSLASH && nextByte == DOT) {
				result.append((char)b);
			} else {
				result.append((char)b);				
			}
			pos++;
		}
		
		return result.toString();
	}
}
