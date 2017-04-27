package com.liquidlabs.common;

import com.carrotsearch.hppc.CharArrayList;
import com.google.common.base.Splitter;
import com.liquidlabs.common.collection.Arrays;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TCharArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class StringUtil {
    private static final String BACK_SLASH = "\\";
    private static final String SPECIAL_CHARS = "([{^-$|]})?*+.";

    private static final String SPACE = " ";

    /**
     * Create a String from a char array with zero-copy (if available), using reflection to access a package-protected constructor of String.
     * */

//    private static final Constructor<String> stringConstructor = getProtectedConstructor(String.class, int.class, int.class, char[].class);
    private static final Constructor<String> stringConstructor = getProtectedConstructor(String.class, char[].class, boolean.class);//int.class, int.class, char[].class);

    public static final String wrapCharArray(char[] c)  {
        if (stringConstructor != null)   {
            try {
//                return stringConstructor.newInstance(0, c.length, c);
                return stringConstructor.newInstance(c, false);
            } catch (Exception e)  { }
        } else {
            return new String(c);
        }
        return null;
    }
    private static Constructor getProtectedConstructor(Class klass, Class... paramTypes) {
        Constructor c;
        try
        {
            c = klass.getDeclaredConstructor(paramTypes);
            c.setAccessible(true);
            return c;
        }
        catch (Exception e)
        {
            return null;
        }
    }

    public static String replaceAll(String[] replaceChars, String withChar, String rawString){
        String replacedString = rawString;
        for(int i = 0; i < replaceChars.length; i++){
            replacedString = replacedString.replace(replaceChars[i], withChar);
        }

        return replacedString;
    }

    // round to 5 decimal places
    public static double roundDouble(double value) {
        return (double) roundDouble(value, 5);
    }
    // round to 2 decimal places
    public static double roundDouble(double value, int places) {
        double magnitude = Math.pow(10,places);
        return Math.round(value*magnitude)/magnitude;
    }

     public static String truncateString(int length, String srcString){
        return srcString.length() < length ? srcString : srcString.substring(0, length-10) + "..." + srcString.substring(srcString.length() - 10, srcString.length()) + "[Truncated]";
     }

    public static boolean containsNumber(String str) {
        char[] chars = str.toCharArray();
        for (char aChar : chars) {
            if (aChar >= '0' && aChar <= '9') return true;
        }
        return false;
    }
    public static String doubleToString(Double dub) {
        StringBuilder s = new StringBuilder();
        DoubleToString.append(s, dub);
        String string = s.toString();
        if (string.endsWith(".0")) string = string.substring(0, string.length() - (".0".length()));
        return string;
    }

    private static final String ATT = "@";

    public static boolean isDoubleSpaced(String text) {
        String[] massagedLines = text.toString().split("\n");
        if (massagedLines.length == 1 && text.contains("\r")) massagedLines = text.split("\r");
        boolean lastLineHasSize = true;
        int zerolength = 0;

        for (String line : massagedLines) {
            if (line.trim().length() > 0) {
                lastLineHasSize = true;
            } else {
                // dont count consecutive empty lines
                if (lastLineHasSize) {
                    zerolength++;
                }
                lastLineHasSize = false;
            }
        }
        return zerolength > 1;
    }

    public static String escapeAsText(String line) {
        StringBuilder result = new StringBuilder();
        StringCharacterIterator iter = new StringCharacterIterator(line);

        for (char ch = iter.first(); ch != java.text.CharacterIterator.DONE; ch = iter.next()) {
            if (Character.isLetterOrDigit(ch)) {
                result.append(ch);
            } else if (Character.isWhitespace(ch)) {
                result.append(ch);
            } else if (ch >=  32 && ch <= 126 || ch == 9 || ch == 10) {
                result.append(ch);
            } else if (ch == '\n') {
                result.append(ch);
            } else {
                result.append(ATT).append(Integer.toString(ch));
            }
        }


        return result.toString();
    }

    static Map<String, Pattern> replacers = new HashMap<String, Pattern>();

    public static String[] fixForXML(String[] source) {
        String[] results = new String[source.length];
        for (int i = 0; i < source.length; i++) {
            results[i] = fixForXML(source[i]);
        }
        return results;
    }

    static public String fixForXML(String text) {
        if (text == null) return text;
        synchronized  (replacers) {
            if (replacers.size() == 0) {
                //"[ :+\\.]+"
                String[] replaceFr = new String[] { "<"   , ">", "'", "'", "&" };
                String[] replaceTo = new String[] { "&lt;", "gt;", "&quot;", "&apos", "&amp;"};

                for (int i = 0; i < replaceFr.length; i++) {
                    Pattern p1 = Pattern.compile(replaceFr[i]);
                    replacers.put(replaceTo[i], p1);
                }
            }
        }
        for (String replaceTo : replacers.keySet()) {
            text = replacers.get(replaceTo).matcher(text).replaceAll(replaceTo);
        }
        return text;
    }


    public static TLongHashSet toTroveLongSet(final java.util.Collection<String> values) {
        TLongHashSet results = new TLongHashSet(values.size());
        for (String string : values) {
            results.add(encodeStringAsLong(string));
        }
        return results;
    }
    public static long encodeStringAsLong(final String key) {
        final int length = key.length();
        if (length > 8) {
            return MurmurHash3.hashString(key, 2);
        }
        long result = 0;
        for (int i = 0; i < length; i++) {
            result += ((long) ((byte) key.charAt(i))) << i * 8;
        }
        return result;
    }



//	/**
//	 *
//	 * WARNING _ do not use this method - it buggers up the heap
//	 * @param sb
//	 * @return
//	 */
//	public static String zeroCopyString(StringBuilder sb) {
//		if (countSB == null) return sb.toString();
//		String result = "";
//		try {
//			char[] contents = (char[]) valueFieldSB.get(sb);
//			int count = countSB.getInt(sb);
//			valueFieldString.set(result, contents);
//			countString.set(result, count);
//			return result;
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return sb.toString();
//	}

    public static boolean OLDisInteger(String str) {
        if (str == null) {
            return false;
        }
        int length = str.length();
        if (length == 0) {
            return false;
        }
        int i = 0;
        if (str.charAt(0) == '-') {
            if (length == 1) {
                return false;
            }
            i = 1;
        }
        for (; i < length; i++) {
            char c = str.charAt(i);
            if (c <= '/' || c >= ':') {
                return false;
            }
        }
        return true;
    }
    public static Long isLong(String str) {
        try {
            return  longValueOf(str);
        } catch (Throwable t) {}
        return null;
    }
    public static Integer isInteger(String str) {
        long ival = 0, idx = 0, end;
        boolean sign = false;
        char ch;

        if( str == null || ( end = str.length() ) == 0 ||
                ( ( ch = str.charAt( 0 ) ) < '0' || ch > '9' )
                        && ( !( sign = ch == '-' ) || ++idx == end || ( ( ch = str.charAt((int) idx) ) < '0' || ch > '9' ) ) )
            return null;

        for(;; ival *= 10 )
        {
            ival += '0'- ch;
            if( ++idx == end )  {
                long rr = sign ? ival : -ival;
                if (rr > Integer.MAX_VALUE) return null;
                return (int) rr;
            }
            if( ( ch = str.charAt((int) idx) ) < '0' || ch > '9' )
                return null;
        }
    }
    public static boolean isIntegerFast(String string) throws IllegalArgumentException {
        return isInteger(string) != null;
    }
    public static int intValueOf( String str ){
        int ival = 0, idx = 0, end;
        boolean sign = false;
        char ch;

        if( str == null || ( end = str.length() ) == 0 ||
                ( ( ch = str.charAt( 0 ) ) < '0' || ch > '9' )
                        && ( !( sign = ch == '-' ) || ++idx == end || ( ( ch = str.charAt( idx ) ) < '0' || ch > '9' ) ) )
            throw new NumberFormatException( str );

        for(;; ival *= 10 )
        {
            ival += '0'- ch;
            if( ++idx == end )
                return sign ? ival : -ival;
            if( ( ch = str.charAt( idx ) ) < '0' || ch > '9' )
                throw new NumberFormatException( str );
        }
    }
    public static long longValueOf( String str ){
        long ival = 0, idx = 0, end;
        boolean sign = false;
        char ch;

        if( str == null || ( end = str.length() ) == 0 ||
                ( ( ch = str.charAt( 0 ) ) < '0' || ch > '9' )
                        && ( !( sign = ch == '-' ) || ++idx == end || ( ( ch = str.charAt((int) idx) ) < '0' || ch > '9' ) ) )
            throw new NumberFormatException( str );

        for(;; ival *= 10 )
        {
            ival += '0'- ch;
            if( ++idx == end )
                return sign ? ival : -ival;
            if( ( ch = str.charAt((int) idx) ) < '0' || ch > '9' )
                throw new NumberFormatException( str );
        }
    }

    public static boolean isIntegerFastOLD(String string) throws IllegalArgumentException {
        boolean isnumeric = false;

        if (string != null && !string.equals("")) {
            isnumeric = true;
            string.toCharArray();
            char chars[] = string.toCharArray();

            for (int d = 0; d < chars.length; d++) {
                isnumeric &= Character.isDigit(chars[d]);

                if (!isnumeric)
                    break;
            }
        }
        return isnumeric;
    }


    public static String removeControlChars(String srcData) {
        String string = new String(srcData.getBytes());
        StringBuilder stringBuilder = new StringBuilder(string.length());
        byte[] bytes = string.getBytes();
        for (byte b : bytes) {
            if (b < 40) stringBuilder.append(SPACE);
            else stringBuilder.append((char)b);
        }

        return stringBuilder.toString();

    }

    public static String substring(String src, String fromToken, String toToken) {
        int from = src.indexOf(fromToken);
        if (from == -1) return null;
        from += fromToken.length();
        int to = src.indexOf(toToken, from);
        if (to == -1) {
            return src.substring(from);
        }
        return src.substring(from, to);
    }
    public static String lastSubstring(String src, String fromToken, String toToken) {
        int from = src.lastIndexOf(fromToken);
        if (from == -1) return null;
        from += fromToken.length();
        int to = src.lastIndexOf(toToken, from);
        if (to == -1) {
            return src.substring(from);
        }
        // if looking for /val/log/etc - and trying to get the log bit then the indexes will be the same -
        // we need to look back before
        int tryIndex = 0;
        int spin = 0;
        if (from >= to) {
            while (tryIndex < to && spin++ < 10 * 1000) {
                tryIndex = src.indexOf(fromToken, tryIndex+1);
                if (tryIndex == -1) return null;
                if (tryIndex < to ) from = tryIndex+1;
            }
        }
        if (spin > 9000) return null;
        if (to < from) to = src.length();
        return src.substring(from, to);
    }

    public static String replaceSection(String source, String fromTag,
                                        String toTag, String replaceWith) {
        int fromOffset = source.indexOf(fromTag);
        int toOffset = source.indexOf(toTag, fromOffset);
        String before = source.substring(0, fromOffset + fromTag.length());
        String after = source.substring(toOffset, source.length());

        return before + replaceWith + after;
    }

    public static Number isNumber(String value) {
        boolean integerFast = isIntegerFast(value);
        if (integerFast) return isInteger(value);
        return isDouble(value);

    }
    public static Double isDouble(String value)
    {
        if (value == null || value.length() == 0) return null;
        boolean seenDot = false;
        boolean seenExp = false;
        boolean justSeenExp = false;
        boolean seenDigit = false;
        for (int i=0; i < value.length(); i++)
        {
            char c = value.charAt(i);
            if (c >= '0' && c <= '9')
            {
                seenDigit = true;
                continue;
            }
            if ((c == '-' || c=='+') && (i == 0 || justSeenExp))
            {
                continue;
            }
            if (c == '.' && !seenDot)
            {
                seenDot = true;
                continue;
            }
            justSeenExp = false;
            if ((c == 'e' || c == 'E') && !seenExp)
            {
                seenExp = true;
                justSeenExp = true;
                continue;
            }
            return null;
        }
        if (!seenDigit)
        {
            return null;
        }
        try
        {
            return Double.parseDouble(value);
        }
        catch (NumberFormatException e)
        {
            return null;
        }
    }

    /**
     * Use during externalization methods....FAST as it avoids charset encoding
     * From http://www.javacodegeeks.com/2010/11/java-best-practices-char-to-byte-and.html
     * @param str
     * @return
     */
    public static byte[] stringToBytesUTFCustom(String str) {
        char[] buffer = str.toCharArray();
        byte[] b = new byte[buffer.length << 1];
        for (int i = 0; i < buffer.length; i++) {
            int bpos = i << 1;
            b[bpos] = (byte) ((buffer[i] & 0xFF00) >> 8);
            b[bpos + 1] = (byte) (buffer[i] & 0x00FF);
        }
        return b;
    }
    /**
     * Use during externalization methods....FAST as it avoids charset encoding
     * From http://www.javacodegeeks.com/2010/11/java-best-practices-char-to-byte-and.html
     * @param str
     * @return
     */
    public static String bytesToStringUTFCustom(byte[] bytes) {
        char[] buffer = new char[bytes.length >> 1];
        for (int i = 0; i < buffer.length; i++) {
            int bpos = i << 1;
            char c = (char) (((bytes[bpos] & 0x00FF) << 8) + (bytes[bpos + 1] & 0x00FF));
            buffer[i] = c;
        }
        return new String(buffer);
    }
    public static byte[] stringToBytesASCII(String str) {
        char[] buffer = str.toCharArray();
        byte[] b = new byte[buffer.length];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) buffer[i];
        }
        return b;
    }


    public static boolean isHex(String string) {
        try {
            Integer.parseInt(string, 16);
            return true;
        } catch (Throwable t) {

        }
        return false;
    }


    public static String[] split(String token, String stringToSplit) {
        return splitRE(token, stringToSplit);
    }

    private static Map<String, Pattern> cachedSplits = new ConcurrentHashMap<String, Pattern>();

    public static String[] splitRE(String token, String stringToSplit){

        try {
            if (SPECIAL_CHARS.contains(token)) {
                token = BACK_SLASH + token;
            }
            if (!cachedSplits.containsKey(token)) {
                cachedSplits.put(token, Pattern.compile(token));
            }
            return cachedSplits.get(token).split(stringToSplit);
        } catch (Throwable t) {
        }
        return null;
    }
    /**
     * Over 2x faster than normal string.split
     * @param data
     * @param splitChar
     * @param length
     * @param includeGroupZero
     * Will greedily grab the remainder of the line as well
     * @return
     */
    public static String[] splitFast(String data, char splitChar, int length, boolean includeGroupZero) {
        int i = 0;
        String[] result = new String[includeGroupZero ? length + 1 : length];
        int pos = 0;
        if (includeGroupZero) {
            result[0] = data;
            pos++;
        }
        int dlength = data.length();
        while (i <= dlength) {
            int start = i;
            while (i < dlength && data.charAt(i) != splitChar) {
                i++;
            }
            // greedily grab the remainder
            if (pos >= result.length-1) {
                result[pos] = data.substring(start);
                i = dlength;
            } else {
                result[pos] = data.substring(start, i);
            }
            i++;
            pos++;
        }
        // didnt get a full set of values - trim to valid length instead of returning null strings
        if (pos < result.length) {
            return Arrays.subArray(result, 0, pos);
        }
        return result;
    }

    /**
     * Used for processing direct-mem bbs
     * @param bytes
     * @param from
     * @param length
     * @param splitChar
     */
    public static String[] splitFast(String data, char splitChar) {
        return splitFast1(data, splitChar);
    }

    public static String[] splitFast5(String data, char splitChar) {

        int i = 0;
        int foundChars = 0;
        int dataLength = data.length();
        TIntArrayList markers = new TIntArrayList();

        for (i = 0; i < dataLength; i++) {
            if (data.charAt(i) == splitChar) {
                foundChars++;
                markers.add(i);
            }
        }

        if (foundChars == 0) return new String[] { data };
        String[] result = new String[foundChars+1];
        int pos = 0;
        i = 0;
        while (i <= dataLength) {
            int start = i;
//            while (i < dataLength && data.charAt(i) != splitChar) {
//                i++;
//            }
            result[pos] = data.substring(start, i);
            i++;
            pos++;
        }
        return result;
    }


    public static String[] splitFast4(String data, char splitChar) {

        int i = 0;
        int foundChars = 0;
        int dataLength = data.length();
        for (i = 0; i < dataLength; i++) {
            if (data.charAt(i) == splitChar) foundChars++;
        }

        if (foundChars == 0) return new String[] { data };
        String[] result = new String[foundChars+1];
        int pos = 0;
        i = 0;
        while (i <= dataLength) {
            int start = i;
            while (i < dataLength && data.charAt(i) != splitChar) {
                i++;
            }
            result[pos] = data.substring(start, i);
            i++;
            pos++;
        }
        return result;
    }

    public static String[] splitFast3(String data, char splitChar) {

        if (data == null) {
            return new String[0];
        }

        List<String> result = new ArrayList<String>();
        int last = 0;
        int dataLength = data.length();
        for (int i = 0; i < dataLength; i++) {
            if (data.charAt(i) == splitChar) {
                result.add(data.substring(last, i));
                last = i +1;
            }
        }

        return result.toArray(new String[0]);
    }


    /**
     * Less reliable than 'splitFast' but about 30% faster
     * Expects A!B!C!D! - i.e. must end with split Token
     * @param data
     * @param splitChar
     * @return
     */
    public static String[] splitFast2(String data, char splitChar) {

        if (data == null) return new String[0];

        int i = 0;
        int foundChars = 0;
        int markerLimit = 512;
        int[] markers = new int[markerLimit];
        int markerCount = 0;
        int dataLength = data.length();
        for (i = 0; i < dataLength; i++) {
            if (data.charAt(i) == splitChar && markerCount < markerLimit) {
                foundChars++;
                markers[markerCount++] = i;
            }
        }

        String[] result = new String[foundChars];

        int last = 0;
        int pos = 0;
        i = 0;
        for (i = 0; i < markerCount; i++) {
            int next = markers[i];
            result[pos++] = data.substring(last, next);
            last = next+1;
        }
        return result;
    }


    /**
     * The original, pretty fast, but performs safety checking etc so a bit slower than version 2.0
     * @param data
     * @param splitChar
     * @return
     */
    public static String[] splitFast1(String data, char splitChar) {

        if (data == null) {
            return new String[0];
        }
        int i = 0;
        int foundChars = 0;
        int dataLength = data.length();
        for (i = 0; i < dataLength; i++) {
            if (data.charAt(i) == splitChar) foundChars++;
        }

        if (foundChars == 0) return new String[] { data };
        String[] result = new String[foundChars+1];
        int pos = 0;
        i = 0;
        while (i <= dataLength) {
            int start = i;
            while (i < dataLength && data.charAt(i) != splitChar) {
                i++;
            }
            // greedily grab the remainder
            if (pos >= result.length-1) {
                result[pos] = data.substring(start);
                i = dataLength;
            } else {
                result[pos] = data.substring(start, i);
            }
            i++;
            pos++;
        }
        // didnt get a full set of values - trim to valid length instead of returning null strings
        if (pos < result.length) {
            return Arrays.subArray(result, 0, pos);
        }
        return result;
    }

//    public static String[] splitFast(char[] data, char splitChar) {
//        int foundChars = 0;
//        if (data == null) {
//            return new String[0];
//        }
//        for (int i = 0; i < data.length; i++) {
//            if (data[i] == splitChar) foundChars++;
//        }
//        int i = 0;
//        if (foundChars == 0) return new String[] { data };
//        String[] result = new String[foundChars+1];
//        int pos = 0;
//        while (i <= data.length()) {
//            int start = i;
//            while (i < data.length() && data.charAt(i) != splitChar) {
//                i++;
//            }
//            // greedily grab the remainder
//            if (pos >= result.length-1) {
//                result[pos] = data.substring(start);
//                i = data.length();
//            } else {
//                result[pos] = data.substring(start, i);
//            }
//            i++;
//            pos++;
//        }
//        // didnt get a full set of values - trim to valid length instead of returning null strings
//        if (pos < result.length) {
//            return Arrays.subArray(result, 0, pos);
//        }
//        return result;
//    }

    public static String splitFastSCAN(String data, char splitChar, int getItem) {
        int i = 0;
        int pos = 1;
        int length = data.length();
        while (i <= length) {
            int start = i;
            while (i < length && data.charAt(i) != splitChar) {
                i++;
            }
            if (pos == getItem) return data.substring(start, i);
            i++;
            pos++;
        }
        return "";
    }

    public static boolean containsAny(String str, String[] searchStrings) {
        for (int i = 0; i < searchStrings.length; i++) {
            if (str.contains(searchStrings[i])) return true;
        }
        return false;
    }

    public static boolean containsIgnoreCase(String value, String contains) {
        if(value == null || contains == null || contains.length() > value.length()) return false;
        if (contains.length() == 0) return true;
        int max = value.length() - contains.length();

        final char first = contains.charAt(0);
        for(int i = 0; i<= max; i++) {
            if(!isEqualChar(value.charAt(i), first)) continue;

            if(restMatches(value, contains, i)) return true;
        }

        return false;
    }

    public static boolean containsIgnoreCase2(String value, String contains) {
        if(contains.length() > value.length()) return false;
        int max = value.length() - contains.length();

        final char first = contains.charAt(0);
        for(int i = 0; i<= max; i++) {
            if(!isEqualChar2(value.charAt(i), first)) continue;

            if(restMatches2(value, contains, i)) return true;
        }

        return false;
    }

    public static boolean containsIgnoreCase3(String value, String contains) {
        if(contains.length() > value.length()) return false;
        int max = value.length() - contains.length();

        final char first = contains.charAt(0);
        for(int i = 0; i<= max; i++) {
            if(!isEqualChar3(value.charAt(i), first)) continue;

            if(restMatches3(value, contains, i)) return true;
        }

        return false;
    }

    private static boolean restMatches(String val, String cont, int i) {
        for(int j = 1; j < cont.length(); j++) {
            if(!isEqualChar(val.charAt(i+j), cont.charAt(j))) return false;
        }
        return true;
    }

    private static boolean restMatches2(String val, String cont, int i) {
        for(int j = 1; j < cont.length(); j++) {
            if(!isEqualChar2(val.charAt(i+j), cont.charAt(j))) return false;
        }
        return true;
    }

    private static boolean restMatches3(String val, String cont, int i) {
        for(int j = 1; j < cont.length(); j++) {
            if(!isEqualChar3(val.charAt(i+j), cont.charAt(j))) return false;
        }
        return true;
    }

    final static char [] ALPHA_ASCII= {
            1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,
            'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','[','\\',']', '^', '_', '`',
            'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'
    };

    final static int ALPHA_LENGTH = ALPHA_ASCII.length;

    private static boolean isEqualChar(int c, int c1) {
        return c == c1 || c < ALPHA_LENGTH && c1 < ALPHA_LENGTH && ALPHA_ASCII[c] == ALPHA_ASCII[c1];
    }

    private static boolean isEqualChar2(char c, char c1) {
        if (c == c1) return true;
        if (c >= 'A' &&  c <= 'Z') c = (char) ('a' + c - 'A');
        if (c1 >= 'A' && c1 <= 'Z') c1 = (char) ('a' + c1 - 'A');
        return c == c1;
    }

    private static boolean isEqualChar3(char c, char c1) {
        if(c==c1) {
            return true;
        }
        return Character.toLowerCase(c) == Character.toLowerCase(c1);
    }


    public static Splitter getCommaSplitter() {
        return  Splitter.on(',').omitEmptyStrings().trimResults();
    }
    public static int indexOf(String nextLine, char c) {
        int length = nextLine.length();
        for (int i = 0; i < length; i++) {
            if (c == nextLine.charAt(i)) return i;
        }
        return -1;
    }

    public static int indexOf(String nextLine, char[] chars) {
        int length = nextLine.length();
        for (int i = 0; i < length; i++) {
            for (char c : chars) {
                if (c == nextLine.charAt(i)) return i;
            }
        }

        return -1;
    }
    public static int indexOfCharRange(String nextLine, boolean include, char[] chars) {
//        char[] chars1 = nextLine.toCharArray();
        int length = nextLine.length();
        for (int i = 0; i < length; i++) {
            char cc = nextLine.charAt(i);
            boolean isOutOfRange = (cc < chars[0] || cc > chars[1]);
            if (include && isOutOfRange) return i;
            else if (!include && isOutOfRange) return i;
        }
        return -1;
    }

    public static String escapeXMLEntities(String sourceXML) {
        StringBuilder result = new StringBuilder();
        boolean wasLastACloseXML = false;
        for (int i = 0; i < sourceXML.length(); i++) {
            char c = sourceXML.charAt(i);
            if (c == '>') {
                wasLastACloseXML = true;
                result.append(c);
            } else if (c == '<') {
                wasLastACloseXML = false;
                result.append(c);
            } else if (wasLastACloseXML && c == '&') {
                result.append("&amp;");
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    public static String[] trim(String[] split) {
        String[] results = new String[split.length];
        for (int i = 0; i < split.length; i++) {
            results[i] = split[i].trim();
        }
        return results;
    }
}
