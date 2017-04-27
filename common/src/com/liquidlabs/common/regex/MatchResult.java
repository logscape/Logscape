package com.liquidlabs.common.regex;

import jregex.Matcher;

import org.apache.oro.text.regex.Perl5Matcher;

public class MatchResult {
	String[] groups;
	boolean match;
	public int getGroupCount() {
		return groups.length;
	}
	final public String getGroup(int i) {
		// ask for 0 then length =1, 
		// so if i >= length (0 >= 1 - fail 
		if (i >= groups.length) return null;
		return groups[i];
	}
	
	public MatchResult() {
	}
	public MatchResult(String... groups){
		this.groups = groups;
		if (groups.length > 0) match = true;
	}
	/**
	 * Native Java RegEx version
	 */
	public MatchResult(final java.util.regex.Matcher matcher, final boolean isMultiline) {
		if (isMultiline) {
			handleMultilineMatch(matcher);
		} else {
			handleSingleLineMatch(matcher);
		}

	}
	
	final private void handleSingleLineMatch(final java.util.regex.Matcher matcher) {
		if (matcher.matches()) {
			int groupCount = matcher.groupCount();
			if (groupCount > 0) groupCount++;
			this.groups = new String[groupCount];
			copyGroupsFromMatcher(matcher, groupCount);
			this.match = true;
		}
	}
	
	final private void copyGroupsFromMatcher(final java.util.regex.Matcher matcher, final int groupCount) {
		for (int i = 0; i < groupCount; i++) {
			groups[i] = matcher.group(i);
		}
	}
	final private void handleMultilineMatch(java.util.regex.Matcher matcher) {
		if (!matcher.matches()) return;
		if (matcher.find()) {
			int groupCount = matcher.groupCount();
			this.groups = new String[groupCount];
			copyGroupsFromMatcher(matcher, groupCount);
			this.match = true;
		} else
			handleSingleLineMatch(matcher);

		
	}
	
	// ================================================================================
	/**
	 * JRegEx Engine
	 * @param match
	 * @param isMultiline
	 */
	public MatchResult(Matcher match, boolean isMultiline) {
		if (isMultiline) {
			handleMultilineMatch(match);
		} else {
			handleSingleLineMatch(match);
		}
	}
	
	/**
	 * Apache ORO
	 * @param matcher
	 * @param matches
	 */
	public MatchResult(Perl5Matcher matcher, boolean matches, boolean skipGroup0) {
		if (matches) {
			org.apache.oro.text.regex.MatchResult match2 = matcher.getMatch();
			if (skipGroup0) {
				int groupCount = match2.groups();
				this.groups = new String[groupCount-1];
				for (int i = 1; i < groupCount; i++) {
					groups[i-1] = match2.group(i);
				}
			} else {
			
				int groupCount = match2.groups();
				this.groups = new String[groupCount];
				for (int i = 0; i < groupCount; i++) {
					groups[i] = match2.group(i);
				}
			}
			this.match = true;
		}
	}
	final private void handleSingleLineMatch(Matcher match) {
		if (match.matches()) {
			groups = match.groups();
			this.match = true;
		} else groups = new String[0];
	}
	final private void handleMultilineMatch(Matcher match) {
		if (!match.matches()) return;
		if (match.find()) {
			groups = match.groups();
			this.match = true;
		} else
			handleSingleLineMatch(match);
	}
	final public int groups() {
		return groups != null ? groups.length : 0;
	}
	final public String group(int i) {
		return groups[i];
	}
	final public String[] getGroups() {
		return groups;
	}
	final public boolean isMatch() {
		return match;
	}
	
	public String toString() {
		int pos = 0;
		StringBuilder groupsText = new StringBuilder();
		for (String groupText : groups) {
			groupsText.append("\n").append(pos++).append(") ").append(groupText);
		}
			
		return String.format("%s match:%b groupCount:%d groups:%s", getClass().getSimpleName(), match, groups.length, groupsText.toString());
	}
	final public void setMatch(boolean match) {
		this.match = match;
		
	}
	public void setGroups(String[] strings) {
		this.groups = strings;
	}

}
