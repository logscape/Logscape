package com.liquidlabs.log.fields;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.common.regex.RegExpUtil;
import com.liquidlabs.log.fields.field.FieldFactory;
import com.liquidlabs.log.fields.field.FieldI;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SynthGuesser implements SynthFieldGenerator{
	private static final Logger LOGGER = Logger.getLogger(SynthGuesser.class);
	
	private static final char CHAR_0 = '0';
	private static final char CHAR_9 = '9';
	List<String> tokenSplitters = Arrays.asList("=",": ", ":");
	private int synthLimit = Integer.getInteger("synth.limit", 5);

    @Override
    public FieldSet guess(FieldSet fieldSet, String... data) {
        return guessSynthFields(fieldSet, data);
    }

	public FieldSet guessSynthFields(FieldSet fieldSet, String[] lines) {
		List<FieldI> fields = fieldSet.fields();
		Set<String> synthIds = new HashSet<String>();
		Map<String, AtomicInteger> synthHits = new HashMap<String, AtomicInteger>();
		Map<String, FieldI> allSynths = new HashMap<String, FieldI>();
		int lineNum = 0;
		for (FieldI field : fields) {
			for (String line : lines) {
				try {
					String fieldValue = fieldSet.getFieldValue(field.name(), fieldSet.getFields(line, -1, -1, -1));
					if (lineNum++ == 0 && line.startsWith("#")) continue;
					for (String token : tokenSplitters) {
						List<FieldI> guessSynthField = guessSynthField(field.name(), fieldValue, token, synthIds, synthIds.size() < 5);
						for (FieldI sf : guessSynthField) {
							if (!synthHits.containsKey(sf.name())) synthHits.put(sf.name(), new AtomicInteger());
							synthHits.get(sf.name()).incrementAndGet();
							allSynths.put(sf.name(), sf);
						}
					}
				} catch (Throwable t) {
					LOGGER.warn("Failed to get Fields from:" + line, t);
				}
			}
		}
		List<FieldI> topFields = getTopFields(synthHits, allSynths);
		fieldSet.fields().addAll(topFields);
		return fieldSet;
	}
	
	private List<FieldI> getTopFields(final Map<String, AtomicInteger> synthHits, Map<String, FieldI> allSynths) {
		List<FieldI> results = new ArrayList<FieldI>(allSynths.values());
		Collections.sort(results, new Comparator<FieldI>(){
			public int compare(FieldI o1, FieldI o2) {
				AtomicInteger o1hits = synthHits.get(o1.name());
				AtomicInteger o2hits = synthHits.get(o2.name());
				return Integer.valueOf(o2hits.get()).compareTo(o1hits.get());
			}
		});
		if (results.size() > 5) return results.subList(0, 5);
		return results;
	}

	Set<String> ignoreThese = new HashSet<String>(Arrays.asList("http", "HTTP","tcp","udp","stcp","https"));
	
	List<FieldI> guessSynthField(String fieldName, String fieldValue, String synthToken, Set<String> synthIds, boolean shouldSummary) {
		List<FieldI> results = new ArrayList<FieldI>();
		if (fieldValue == null) return results;
		if (fieldValue.split(synthToken).length > 1) {
			String[] split = fieldValue.split(synthToken);
			
			for (int i = 0; i < split.length -1; i++) {
				String[] parts0 = split[i].split(" ");
				if (parts0.length == 0) continue;
				
				String key = getLastWordInToken(parts0[parts0.length-1]);
//				System.out.println("Synth:" + fieldName + " Part:" + key);
				
				if (key == null) continue;
				
				if (ignoreThese.contains(key)) continue;
				// ignore numeric keys
				if (key.matches("(\\d+)") || key.matches("(\\d+)\\.(\\d+)")) continue;
				
				// ignore keys with length 1
				if (key.length() < 2) continue;
				
				// looks like a word or something fairly simple - allow - and _
				if (!key.matches("([a-z,A-Z,_,-]+)")) continue;
				
				String[] valuePart = split[i+1].split(" ");
				if (valuePart[0].length() == 0) continue;
				
				boolean isNum = startsWithNumber(valuePart[0]);
//				if (isNum) {
//					results.add(new Field(field.name + "-" + key, "avg(_host)", true, field.name, key + synthToken +"(d)", shouldSummary, 1));					
//				} else 
				{
					String function = "count()";
					if (isNum) function = "avg(_host)";
					String expression = getExpressionForShortestValue(key, synthToken, fieldValue);
					results.add(FieldFactory.getField(fieldName + "-" + key,expression,fieldName,1,true, true,function, false));

//					results.add(new Field(field.name + "-" + key, "count()", true, field.name, key + synthToken +"(^[\\|=&;:,!? \\)_\\/])", shouldSummary, 1));
				}
				synthIds.add(key);
			}
		}
		return results;
	}
	private String getExpressionForShortestValue(String key, String synthToken, String data) {
		ResultItem resultItem = new ResultItem();
		
		MatchResult andSplit= RegExpUtil.matches(".*" + key + synthToken + "([^&]+).*",data);
		resultItem.check(andSplit,String.format("substring,%s%s,%s", key, synthToken, "&"));
		
		
		MatchResult pipeSplit= RegExpUtil.matches(".*" + key + synthToken + "([^\\|]+).*",data);
		resultItem.check(pipeSplit, String.format("substring,%s%s,%s", key, synthToken, "|"));
		
		MatchResult semiSplit= RegExpUtil.matches(".*" + key + synthToken + "([^;]+).*",data);
		resultItem.check(semiSplit, String.format("substring,%s%s,%s", key, synthToken, ";"));
		
		MatchResult colonSplit= RegExpUtil.matches(".*" + key + synthToken + "([^:]+).*",data);
		resultItem.check(colonSplit, String.format("substring,%s%s,%s", key, synthToken, ":"));
		
		MatchResult commaSplit= RegExpUtil.matches(".*" + key + synthToken + "([^,]+).*",data);
		resultItem.check(commaSplit, key + synthToken + "([^,]+)");
		
		MatchResult qmarkSplit= RegExpUtil.matches(".*" + key + synthToken + "([^?]+).*",data);
		resultItem.check(qmarkSplit, String.format("substring,%s%s,%s", key, synthToken, "?"));
		
		MatchResult parenSplit= RegExpUtil.matches(".*" + key + synthToken + "([^\\)]+).*",data);
		resultItem.check(parenSplit, String.format("substring,%s%s,%s", key, synthToken, ")"));
		
		MatchResult rbrktplit= RegExpUtil.matches(".*" + key + synthToken + "([^\\]]+).*",data);
		resultItem.check(rbrktplit, String.format("substring,%s%s,%s", key,synthToken, "]"));
		
		MatchResult spaceSplit= RegExpUtil.matches(".*" + key + synthToken + "(\\S+).*",data);
		resultItem.check(spaceSplit, String.format("substring,%s%s,%s", key,synthToken, " "));
		
		MatchResult dashSplit= RegExpUtil.matches(".*" + key + synthToken + "([^-]+).*",data);
		if (!resultItem.wasMatched) resultItem.check(dashSplit, String.format("substring,%s%s,%s", key,synthToken, "-"));
		
		if (resultItem.wasMatched) return resultItem.resultExpr;
		
		MatchResult wordSplit= RegExpUtil.matches(".*" + key + synthToken + "(\\w+).*",data);
		resultItem.check(wordSplit, (key + synthToken + "(w)"));
		
		return key + synthToken +"(*)";
	}
	public String getLastWordInToken(String string) {
		MatchResult matches = RegExpUtil.matches(".*?(\\w+)", string);
		if (!matches.isMatch()) return null;
		String group = matches.group(matches.groups()-1);
		if (group.contains("_") && !group.endsWith("_")) return group.substring(group.lastIndexOf("_")+1, group.length());
		return group;
	}


	private boolean startsWithNumber(String string) {
		char charAt = string.charAt(0);
		if (charAt > CHAR_9 || charAt < CHAR_0) {
			return false;
		}
		return true;
	}



    private static class ResultItem {
		String resultExample = "9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999";
		String resultExpr = "";
		boolean wasMatched = false;
		public void check(MatchResult mr, String expr) {
			if (mr.isMatch() && mr.getGroup(1).length() <= resultExample.length()) {
				wasMatched = true;
				this.resultExpr = expr;
				this.resultExample = mr.getGroup(1);
			}
			
		}
	}



}
