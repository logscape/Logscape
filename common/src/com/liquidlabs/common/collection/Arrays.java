package com.liquidlabs.common.collection;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import com.liquidlabs.common.StringUtil;

public class Arrays {
	private static final String COMMA = ",";
	public static final String[] EMPTY_ARRAY = new String[0];

	  public static byte[] copyOfRange(byte abyte0[], int i, int j) {
		int k = j - i;
		if (k < 0) {
			throw new IllegalArgumentException((new StringBuilder()).append(i).append(" > ").append(j).toString());
		} else {
			byte abyte1[] = new byte[k];
			System.arraycopy(abyte0, i, abyte1, 0, Math.min(abyte0.length - i, k));
			return abyte1;
		}
	}

	public static Field[] sortFields(Field[] declaredFields) {
		ArrayList<Field> results = new ArrayList<Field>();
		for (Field field : declaredFields) {
			results.add(field);
		}
		Collections.sort(results, new Comparator<Field>(){
			public int compare(Field o1, Field o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		
		Field[] stuff = new Field[declaredFields.length];
		int i = 0;
		for (Field field : results) {
			stuff[i++] = field;
		}
		return stuff;
	}

	public static String toString(Object aobj[]) {
		if (aobj == null)
			return "null";
		int end = aobj.length - 1;
		if (end == -1)
			return "";
		StringBuilder stringbuilder = new StringBuilder();
		int pos = 0;
		do {
			Object current = aobj[pos];
			if (current == null) {
				stringbuilder.append("null");
			} else if (current.getClass().equals(String[].class)) {
				stringbuilder.append(Arrays.toString((String[]) current));
			} else {
				stringbuilder.append(String.valueOf(current));
			}
			if (pos++ == end)
				return stringbuilder.toString();
			stringbuilder.append(COMMA);
		} while (true);
	}
	public static String toStringWithDelim(String delim, Object aobj[]) {
		if (aobj == null)
			return "null";
		int end = aobj.length - 1;
		if (end == -1)
			return "";
		StringBuilder stringbuilder = new StringBuilder();
		int pos = 0;
		do {
			Object current = aobj[pos];
			if (current.getClass().equals(String[].class)) {
				stringbuilder.append(Arrays.toString(aobj));
			} else {
				stringbuilder.append(String.valueOf(current));
			}
			if (pos++ == end)
				return stringbuilder.toString();
			stringbuilder.append(delim);
		} while (true);
	}
	
	public static String toStringWithDelim(String delim, boolean aobj[]) {
		if (aobj == null)
			return "null";
		int end = aobj.length - 1;
		if (end == -1)
			return "";
		StringBuilder stringbuilder = new StringBuilder();
		int pos = 0;
		do {
			Object current = aobj[pos];
			if (current.getClass().equals(String[].class)) {
				stringbuilder.append(Arrays.toString(aobj));
			} else {
				stringbuilder.append(String.valueOf(current));
			}
			if (pos++ == end)
				return stringbuilder.toString();
			stringbuilder.append(delim);
		} while (true);
	}
	
	public static String toString(boolean aobj[]) {
		if (aobj == null)
			return "null";
		int end = aobj.length - 1;
		if (end == -1)
			return "";
		StringBuilder stringbuilder = new StringBuilder();
		int pos = 0;
		do {
			Object current = aobj[pos];
			if (current.getClass().equals(String[].class)) {
				stringbuilder.append(Arrays.toString((String[]) current));
			} else {
				stringbuilder.append(String.valueOf(current));
			}
			if (pos++ == end)
				return stringbuilder.toString();
			stringbuilder.append(COMMA);
		} while (true);
	}
	
	public static String[] split(String token, String stringToSplit){
        if (stringToSplit == null) return new String[0];
		if (token.length() == 1) {
			if (stringToSplit.endsWith(token)) stringToSplit = stringToSplit.substring(0,stringToSplit.length()-1);
			return StringUtil.splitFast(stringToSplit, token.charAt(0));
		} else {
			return StringUtil.splitRE(token, stringToSplit);
		}
	}

	
	public static List<String> asList(String... strings) {
		List<String> result = new ArrayList<String>();
		for (String stringItem : strings) {
			result.add(stringItem);
		}
		return result;
	}
	public static List<Integer> asList(Integer... strings) {
		List<Integer> result = new ArrayList<Integer>();
		for (Integer stringItem : strings) {
			result.add(stringItem);
		}
		return result;
	}

	public static String stringFromStringArray(String[] items){
		StringBuilder stringBuilder = new StringBuilder();
		for (String string : items) {
			stringBuilder.append(string).append(",");
		}
		return stringBuilder.toString();
	}
	
	public static String[] stringArrayFromString(String items){
		ArrayList<String> arrayList = new ArrayList<String>();
		String[] listItem = items.split(COMMA);
		for (String arrayItem : listItem) {
//			arrayItem = arrayItem.replaceAll("\\*", COMMA);
			if (arrayItem.startsWith("[")) {
				arrayItem = arrayItem.replaceAll("\\["," ");
			}
			if (arrayItem.endsWith("]")) {
				arrayItem = arrayItem.replaceAll("\\]"," ");
			}

			arrayList.add(arrayItem);
		}
		return Arrays.toStringArray(arrayList);
	}
	public static String[] ArrayFromStringUsingSplitter(String items){
		
		if (items.length() == 0) return new String[0];
		String[] split = items.split(",");
		String[] results = new String[split.length];
		int pos = 0;

		for (String string : split) {
			results[pos++] = string.replaceAll("\\*", COMMA);
		}
		return results;
	}

	public static String[] toStringArray(Collection<String> list) {
		String[] results = new String[list.size()];
		int i = 0;
		for (String string : list) {
			results[i++] = string;
		}
		return results;
	}

	public static String[] subArray(String[] readKeys, int i, int limit) {
		String[] results = new String[limit - i];
		System.arraycopy(readKeys, i, results, 0, results.length);
		return results;
	}

	public static String append(String... items) {
		return appendWithDelim(COMMA, items);
	}
	final public static String[] append(String[] array, String... items) {
		String[] results = new String[array.length+items.length];
		System.arraycopy(array, 0, results, 0, array.length);
		System.arraycopy(items, 0, results, array.length, items.length);
		return results;
	}
	final public static boolean[] append(boolean[] array, boolean... items) {
		boolean[] results = new boolean[array.length+items.length];
		System.arraycopy(array, 0, results, 0, array.length);
		System.arraycopy(items, 0, results, array.length, items.length);
		return results;
	}

	public static String[] preppend(String[] array, String... items) {
		String[] results = new String[array.length+items.length];
		System.arraycopy(items, 0, results, 0,items.length);
		System.arraycopy(array, 0, results, items.length, array.length);
		return results;
	}

	public static String appendWithDelim(String delimiter, String... items) {
		StringBuilder stringBuilder = new StringBuilder();
		int pos = 0;
		int end = items.length - 1;
		for (String string : items) {
			stringBuilder.append(string);
			if (pos++ != end) stringBuilder.append(delimiter);
		}
		return stringBuilder.toString();
	}
	
	/**
	 * Performs toStirng without the '[' ']' and whitespace business
	 * @param collection
	 * @return
	 */
	public static String toString(Collection<String> collection) {
		StringBuilder stringBuilder = new StringBuilder();
		Iterator<String> iterator = collection.iterator();
		while (iterator.hasNext()) {
			String item = (String) iterator.next();
			stringBuilder.append(item);
			if (iterator.hasNext()) {
				stringBuilder.append(",");
			}
		}
		return stringBuilder.toString();
	}
	
	/**
	 * Usage (make sure you copy the delimiters list cause it will be split as it recurses
	 * private final List<String> delimiters = Arrays.asList(" ", "\t", ",");
	 * return Arrays.recursiveSplit(Arrays.asList(line), new ArrayList<String>(delimiters));
	 */
	public static List<String> recursiveSplit(List<String> line, List<String> delimiters2) {
		if (delimiters2.size() ==0) return line;
		ArrayList<String> result = new ArrayList<String>();
		String cDelim = delimiters2.get(0);
		for (String string : line) {
			String[] split = Arrays.split(cDelim, string);
			for (String splitPart : split) {
				result.add(splitPart);
			}
		}
		return Arrays.recursiveSplit(result, delimiters2.subList(1, delimiters2.size()));
	}

	public static int[] toArray(Object[] array) {
		int[] results = new int[array.length];
		for (int i = 0; i < array.length; i++) {
			results[i] = (Integer) array[i];
		}
		return results;
	}
    public static boolean contains(String[] source, String searchFor) {
        for (String s : source) {
            if (s.equals(searchFor)) return true;
        }
        return false;
    }
	public static int arrayContains(byte[] searchFor, byte[] searchIn, int offset) {
		for (int i = offset; i < searchIn.length; i++) {
			boolean breakOut = false;
			int matchCount = 0;
			for (int j = 0; j < searchFor.length && !breakOut && j+i < searchIn.length; j++) {
				if (searchIn[i+j] == searchFor[j]) {
					matchCount++;
				} else {
					breakOut = true;
				}
			}
			if (matchCount == searchFor.length) {
				return i;
			}
		}
		return -1;
		
	}
	public static int charsContains(char[] searchFor, char[] searchIn, int offset) {
		for (int i = offset; i < searchIn.length; i++) {
			boolean breakOut = false;
			int matchCount = 0;
			for (int j = 0; j < searchFor.length && !breakOut && j+i < searchIn.length; j++) {
				if (searchIn[i+j] == searchFor[j]) {
					matchCount++;
				} else {
					breakOut = true;
				}
			}
			if (matchCount == searchFor.length) {
				return i;
			}
		}
		return -1;
		
	}
	public static int arrayContains(byte[] searchFor, byte[] searchIn) {
		return arrayContains(searchFor, searchIn, 0);
	}

	/**
	 * 
	 * @param searchFor
	 * @param searchIn
	 * @param includeSplitters - when TRUE will the split value and the text in the offset value
	 * @param splitValueAtStart - when TRUE will return the offset start FALSE will return offset end - depends if you include the split/searchFor value
	 * @return
	 */
	public static List<Integer> getSplits(char[] searchFor, char[] searchIn, boolean includeSplitters, boolean splitValueAtStart) {
		int offset = 0;
		ArrayList<Integer> results = new ArrayList<Integer>();
		for (int i = offset; i < searchIn.length; i++) {
			boolean breakOut = false;
			int matchCount = 0;
			for (int j = 0; j < searchFor.length && !breakOut && j+i < searchIn.length; j++) {
				if (searchIn[i+j] == searchFor[j]) {
					matchCount++;
				} else {
					breakOut = true;
				}
			}
			if (matchCount == searchFor.length) {
				if (splitValueAtStart) results.add(i);
				else results.add(i + searchFor.length);
				// skip along the next few chars
				i += searchFor.length-1;
			}
		}
		return results;
	}

}
