package com.liquidlabs.log.links;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.common.regex.RegExpUtil;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import com.liquidlabs.orm.Id;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Link {

    private static final Logger LOGGER = Logger.getLogger(Link.class);

	private static final String SPACE = " ";

	private static final String B_END = "</FONT>";

	private static final String B_START = "<FONT";

	public static String varSplitter = "!";

	// the link name displayed
	@Id
	public String tag;
	
	// the regex filter used to determine if a hyperlink is needed 
	public String linkExpression;
	
	// the name of the search to call onto when the link is executed
	public String searchName;
	
	public int linkGroup = 1;
	public int varGroup = -1;
	
	public Link() {
	}
	
	public Link(String tag, String linkExpression, String searchName, int matchGroup, int varGroup) {
		this.tag = tag;
		this.linkExpression = linkExpression;
		this.searchName = searchName;
		this.linkGroup = matchGroup;
		this.varGroup = varGroup;
	}
	
	transient String regexp;
	transient int handlehit = 0;
	transient Pattern p = null;
	public String handle(String msg) {
		// dont allow the link to be applied more than once

		if (msg.contains(searchName)) return msg;
		
		if (regexp == null) {
			regexp = SimpleQueryConvertor.convertSimpleToRegExp(linkExpression);
		}
		
		if (p == null) {
			p = Pattern.compile(regexp);			
		}
		MatchResult matchResult = new MatchResult(p.matcher(msg), false);
		
		if (matchResult != null && matchResult.isMatch() && matchResult.groups() > 1 && matchResult.getGroupCount() >= linkGroup) {
			try {
			
				handlehit++;
				String group = matchResult.group(linkGroup);
				int offset = msg.indexOf(group);
				if (!linkExpression.startsWith(".") && !linkExpression.startsWith("(")) {
					offset = msg.indexOf(linkExpression.substring(0, linkExpression.indexOf("("))) + linkExpression.indexOf("(");
				}
				int end = offset + group.length();
				if (varGroup <= 0) {
					
					msg = String.format("%s<FONT COLOR='#0000FF'><b><a href='event:_search:%s'>%s</a></b></FONT>%s", msg.substring(0, offset),searchName, group, msg.substring(end, msg.length()));
				} else {
					String varFromMsg = matchResult.group(varGroup);
					msg = String.format("%s<FONT COLOR='#0000FF'><b><a href='event:_search:%s%s%s'>%s</a></b></FONT>%s", msg.substring(0, offset),searchName, varSplitter, varFromMsg, group, msg.substring(end, msg.length()));
				}
			} catch (Throwable t) {
				LOGGER.warn("Failed to apply Link:" + t.toString() + " LINK:" + this.toString());
				return msg;
			}
		}
		return msg;
	}
	/**
	 * Helper stuff for merging lots of linked stuff together
	 * @param lines
	 * @return
	 */
	public String merge(List<Link> links, String line) {
		List<String> linkLines = new ArrayList<String>();
		int hits = 0;
		for (Link link : links) {
			link.handlehit = 0;
            if (line != null) {
                String handle = link.handle(line);
                hits += link.handlehit;
                if (link.handlehit > 0) linkLines.add(handle);
            }
		}
		if (hits == 0) return line;
		return merge(linkLines);
	}
	public String merge(List<String> lines) {
		int tokenCount = 0;
		List<List<String>> allLineVariants = new ArrayList<List<String>>();
		for (String line : lines) {
			List<String> lineParts = getLineParts(line);
			allLineVariants.add(lineParts);
			tokenCount = lineParts.size();
		}
		
		StringBuilder result = new StringBuilder();
		for (int token = 0; token < tokenCount; token++) {
			String longestToken = "";
			for (List<String> lineProcessedForALink : allLineVariants) {
				
				if (lineProcessedForALink.size() <= token) continue;
				if (lineProcessedForALink.get(token).length() > longestToken.length()) longestToken = lineProcessedForALink.get(token);
			}
			result.append(longestToken);
			if (token < tokenCount-1) result.append(SPACE);
		}
		return result.toString();
		
	}

	/**
	 * @param line
	 * @return
	 */
	List<String> getLineParts(String line) {
		
		ArrayList<String> results = new ArrayList<String>();
		String[] split = line.split(SPACE);
		StringBuilder linkPart = new StringBuilder();
		boolean isWithinLink = false;
		for (String splitPart : split) {
			if (!isWithinLink) {
				if (splitPart.contains(B_START)) {
					isWithinLink = true;
					linkPart.append(splitPart);
				} else {
					results.add(splitPart);
				}
			} else {
				// 
				if (splitPart.contains(B_END)) {
					isWithinLink = false;
					linkPart.append(SPACE + splitPart);
					results.add(linkPart.toString());
					linkPart.delete(0, linkPart.length());
				} else {
					linkPart.append(SPACE + splitPart);
				}
			}
		}
		return results;
	}
	public String toString() {
		return String.format("Link:%s exp%s sName:%s mGroup%d vGroup:%d", tag, linkExpression, searchName, linkGroup, varGroup);
	}



}
