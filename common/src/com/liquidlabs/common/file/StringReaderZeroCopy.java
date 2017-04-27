package com.liquidlabs.common.file;

import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.raf.DefaultKeepReadingRule;

import java.util.ArrayList;
import java.util.List;

/**
 * Takes in ByteArrays as sets of String and allows pointer based scanning to return substrings. 
 * The aim is to remove memcopy overhead and rely on String.subString performance
 * 
 * TODO: Still uses too much stack so overhead of getChar() is about 20% - still too slow - see performance in ByteBufferRAF impl by changing readLine() method and running
 * FileScannerBenchMackingTest
 *
 */
public class StringReaderZeroCopy {
	static final char EOL_R = '\r';
	static final char EOL_N = '\n';
	static final int  MINUS_ONE = -1;
	static final int  NOOP = -2;
	private static final int MAX_LINES_PERLINE = Integer.getInteger("raf.mlines.per.event",100);
	private int maxTotalLength = MAX_LINES_PERLINE * 1024;
	
	private BreakRule keepReadingRule = new DefaultKeepReadingRule();

	
	List<String> strings = new ArrayList<String>();
	byte cStringIndex;
	private int cStringOffset;

	public void addString(String string) {
		strings.add(string);
	}

	final public byte readChar() {
		if (cStringIndex >= strings.size()) {
			// pull more data through callback
			String pull = this.stringPuller != null ? this.stringPuller.pull() : null;
			if (pull == null) return MINUS_ONE;
			strings.add(pull);
		}
		String cString = strings.get(cStringIndex);
		int cStringLength = cString.length();
		if (cStringOffset >= cStringLength) {
			move(cStringLength);
			return NOOP;
		}
		char cChar = cString.charAt(cStringOffset);
		cStringOffset++;
		move(cStringLength);
		return (byte) cChar;
	}

	final private void move(final int cStringLength) {
		if (cStringOffset >= cStringLength) {
			cStringOffset = 0;
			cStringIndex++;
		}
	}

	int linesRead;
	private StringPuller stringPuller;
	public String readUntil() {
			removeOldLines();
			
			this.linesRead = 0;
			int charsRead = 0;
			int fromStringIndex = cStringIndex;
			int fromStringOffset = cStringOffset;
			
			byte c = MINUS_ONE;
			boolean eol = false;
			boolean eoinput = false;
			while (!eol && !eoinput) {
				c = readChar();
				if (c == NOOP) continue;
				if (isEndOfInput(c)) {
					eoinput = true;
					continue;
				}
				if (c == EOL_N) {
					linesRead++;
					if (!keepReadingRule.isKeepReading(peek(0), peek(1), peek(2), peek(3)) || linesRead > MAX_LINES_PERLINE || charsRead > maxTotalLength){
						eol = true;				
					}
				} else if (c == EOL_R) {
					linesRead++;
					if (!keepReadingRule.isKeepReading(peek(1), peek(2), peek(3), peek(4)) || linesRead > MAX_LINES_PERLINE || charsRead > maxTotalLength){
						eol = true;
						eol = true;
						while ((readChar()) != EOL_N) {}
					}
				// protect against massive log lines that can cause OOM
				} else if (linesRead > MAX_LINES_PERLINE){
					eol = true;
				} else {
					charsRead++;
				}
			}
			
			if (charsRead == 0) return null;
			
			String startString = strings.get(fromStringIndex);
			return getResults(fromStringIndex, fromStringOffset, startString, eol, eoinput);
	}

	byte peek(int offset) {
		int beforeCStringOffSet = cStringOffset;
		byte beforeCStringIndex = cStringIndex;
		byte result = MINUS_ONE;
		for (int i = 0; i <= offset; i++) {
			byte c = readChar();
			if (i == offset) result = c;
		}
		cStringOffset = beforeCStringOffSet;
		cStringIndex = beforeCStringIndex;
		return result;
	}


	public String readLine() {
		
		removeOldLines();
		int fromStringIndex = cStringIndex;
		int fromStringOffset = cStringOffset;
		byte c = MINUS_ONE;
		boolean eol = false;
		boolean eoinput = false;
		int charsRead = 0;
		
		while (!eol && !eoinput) {
			c = readChar();
			if (c == NOOP) continue;
			
			if (c == MINUS_ONE) {
				eoinput = true;
				continue;
			}
			if (c == EOL_N) {
				eol = true;
			} else if (c == EOL_R) {
				eol = true;
				while ((readChar()) != EOL_N) {
				}
			} else {
				charsRead++;
			}
		}
		if (charsRead == 0 || fromStringIndex >= strings.size()) return null;
		
		String startString = strings.get(fromStringIndex);
		
		return getResults(fromStringIndex, fromStringOffset, startString, eol, eoinput);
	}

	private String getResults(int fromStringIndex, int fromStringOffset, String startString, boolean eol, boolean eoinput) {
		int lastCStringOffset = eol ? cStringOffset -1 : cStringOffset;
		if (fromStringIndex == cStringIndex) {
			return startString.substring(fromStringOffset, lastCStringOffset);
		}
		
//		if (cStringIndex > strings.size()) return startString.substring(fromStringOffset, startString.length()-1);
		
		if (fromStringIndex == cStringIndex -1) {
			int cMax = cStringIndex >= strings.size() ? strings.size()-1 : cStringIndex;
			String cString = strings.get(cMax);
			String part1 = startString.substring(fromStringOffset, startString.length()-1);
			if (cStringOffset == 0) return part1;
			String part2 = cString.substring(0, lastCStringOffset);
			return part1.concat(part2);
		}
		
		
		StringBuilder result = new StringBuilder(startString.substring(fromStringOffset));
		for (int i = fromStringIndex+1; i < cStringIndex-1; i++) {
			String nextString = strings.get(i);
			result.append(nextString);
		}
		// last char was EOL and we rolled to the next line
		if (cStringOffset == 0) {
			String lastString = strings.get(cStringIndex-1);
			if (!eol) result.append(lastString.substring(0, lastString.length()));
			else result.append(lastString.substring(0, lastString.length()-1));
		} else {
			String lastString1 = strings.get(cStringIndex-1);
			result.append(lastString1);
			
			String lastString2 = strings.get(cStringIndex);
			result.append(lastString2.substring(0,	lastCStringOffset));
			
		}
		
		return result.toString();
	}

	private void removeOldLines() {
		if (cStringIndex > 2) {
			for (int i = 0; i < cStringIndex; i++){
				strings.remove(0);
			}
			cStringIndex = 0;
		}
	}

	private boolean isEndOfInput(byte c) {
		return c == MINUS_ONE;
	}
		private boolean isEOLR(byte c) {
			return c == EOL_R;
		}
		
		final private boolean isEOL(final byte c) {
			return c == EOL_N;
		}

		public void registerPuller(StringPuller stringPuller) {
			this.stringPuller = stringPuller;
		}
}
