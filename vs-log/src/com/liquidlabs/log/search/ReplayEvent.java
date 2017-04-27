package com.liquidlabs.log.search;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSet.DEF_FIELDS;
import com.liquidlabs.log.fields.field.FieldI;
import com.liquidlabs.log.fields.field.GroupField;
import com.liquidlabs.orm.Id;
import jregex.Matcher;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.mapdb.Serializer;

import java.io.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class ReplayEvent implements KryoSerializable, Externalizable {


    public enum Mode {
		raw,
		fields,
		structured,
	}

	private final static Logger LOGGER = Logger.getLogger(ReplayEvent.class);

	@Id
	private TimeUID id;

//    public void setTime(long time) {
//		this.time = time;
//	}
	private String subscriber = "";
	private int lineNumber = 0;
	private String sourceURI = "";
	private short querySourceIndex;
	private short groupIndex;
	
	private String rawLineData = "";

	private String[] defaultFieldValues = new String[] { "", "", "", "", "", "", "", "0", "" };
	
	public ReplayEvent() {
        this.id = new TimeUID(1);
    }
	
	public ReplayEvent(String sourceURI, int lineNumber, int querySourceIndex, int groupIndex, String subscriber, long time, String lineData) {
		if (subscriber != null) this.subscriber = subscriber;
		this.sourceURI = sourceURI;
		this.lineNumber = lineNumber;
		this.id = new TimeUID(time);
		this.querySourceIndex = (short) querySourceIndex;
		this.groupIndex = (short) groupIndex;
		this.rawLineData = lineData;
		if (this.rawLineData.length() > 10 * 1024) {
			if (LOGGER.isDebugEnabled()) LOGGER.debug("Truncating LONG line:" + getFilePath() + ":" + lineNumber + " sub:" + subscriber + " length:" + lineData.length());
			this.rawLineData = this.rawLineData.substring(0, 5 * 1024) + "...";
		}
	}
	// test only
	public void setTime(long time) {
        this.id.timeMs = time;
	}


	public TimeUID  getId() {
		return id;
	}
	public long getTime() {
		return id.timeMs;
	}
	public String getHostname() {
		try {
			return defaultFieldValues[FieldSet.DEF_FIELDS._host.ordinal()];
		} catch (Throwable t) {
			return "unknown";
		}
	}
	public String getFilePath() {
		try {
			return defaultFieldValues[FieldSet.DEF_FIELDS._path.ordinal()];
		} catch (Throwable t) {
			return "unknown";
		}
	}

    public String getFilename() {
        return defaultFieldValues[FieldSet.DEF_FIELDS._filename.ordinal()];
    }
    public void setFilename(String filename) {
        defaultFieldValues[FieldSet.DEF_FIELDS._filename.ordinal()] = filename;
    }


    public Integer getLineNumber() {
		return lineNumber;
	}
	public String getSourceURI() {
		return sourceURI;
	}
	public int getQuerySourceIndex() {
		return querySourceIndex;
	}
	public String subscriber() {
		return subscriber;
	}

	public String getRawData() {
		return rawLineData;
	}
	
	public int getGroupIndex() {
		return groupIndex;
	}

	public void setSubscriber(String value) {
		this.subscriber = value;
	}
	public String fieldSetId() {
		return getDefaultField(DEF_FIELDS._type);
	}
	@Override
	public String toString() {
		String fields = java.util.Arrays.toString(this.defaultFieldValues);
		return "Event: fields[" + fields + " " + rawLineData + "]";
	}

	//<data>
//	<row host="s1" file="stuff1111.log" time="25-10-2009 15:00.06"  line="100" text="2010-05-14 16:24:00,736 INFO sla-sched-132-4 (container.SLAContainer)	SLACONTAINER vs-log-1.0_SLA_AggSpace METRICS [Metric threshold=20.0, Metric consumerMin=2.0, Metric consumerUsed=2.0, Metric consumerAlloc=2.0, Metric totalAgents=6.0, Metric cpu=5.0], Metric consumerUsed=2.0, Metric consumerAlloc=2.0, STATUS:RUNNING" />

	public String toXMLReportString(String time, FieldSet fieldSet) {
		String msg = getMsgData(fieldSet, true, true);
		msg = StringEscapeUtils.escapeHtml4(msg);
		
		String file = "";
		String hostname = "";
		String _tag = "";
		String _agent = "";
		
		String[] message = fieldSet.getFields(rawLineData, -1, lineNumber, this.id.timeMs);

		if (fieldSet != null) {
			addDefautFields(fieldSet);
			
			file = fieldSet.getFieldValue("filename", message);
			hostname = fieldSet.getFieldValue("host", message);
			_tag = fieldSet.getFieldValue(FieldSet.DEF_FIELDS._tag.name(), message);
			_agent = fieldSet.getFieldValue(FieldSet.DEF_FIELDS._agent.name(), message);

			if (file == null) file = "";
			if (hostname == null) hostname = "";
			if (_tag == null) _agent = "";
			if (_agent == null) _agent = "";
		}
		int limit = 60;
		if (file.length() > limit) file = "..." + file.substring(file.length() - limit);

		return String.format("<row host=\"%s\" file=\"%s\" time=\"%s\" line=\"%d\" text=\"%s\"/>\n", hostname, file, time, lineNumber, msg);
	}
    public String toCSV(FieldSet fieldSet) {
//        String msg = getMsgData(fieldSet, true, true);
        String msg = StringEscapeUtils.escapeCsv(rawLineData);

            addDefautFields(fieldSet);

        return String.format("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%d\",\"%s\"\n",
                getDefaultField(DEF_FIELDS._host),
                getDefaultField(DEF_FIELDS._path),
                getDefaultField(DEF_FIELDS._filename),
                getDefaultField(DEF_FIELDS._tag),
                getDefaultField(DEF_FIELDS._type),
                id.timeMs, lineNumber, msg);
    }



    public transient List<String> fieldTitles;
	public transient List<String> fieldValues;
	public transient List<String> fieldViews;
	public transient Map<String, String> keyValueMap;
	
	public void reset() {
		fieldTitles = null;
		fieldValues = null;
		fieldViews = null;
		keyValueMap = null;
	}
	
	public String[] getFieldValues(FieldSet fieldSet) {
		return fieldSet.getFields(rawLineData, -1, lineNumber, this.id.timeMs);
	}
	public String populateFieldValues(Set<String> ignoreTheseFieldNames, FieldSet fieldSet) {
		StringBuilder result = new StringBuilder();
		fieldTitles = new CopyOnWriteArrayList<String>();
		fieldValues = new CopyOnWriteArrayList<String>();
		fieldViews = new CopyOnWriteArrayList<String>();
		keyValueMap = new LinkedHashMap<String, String>();

		addDefautFields(fieldSet);
		String[] message = fieldSet.getFields(rawLineData, -1, lineNumber, this.id.timeMs);
		
		for (FieldI fieldItem : fieldSet.fields()) {
			
			String fieldName = fieldItem.name();
			try {
                if (ignoreTheseFieldNames.contains(fieldName)) {
                    continue;
                }

				String fieldValue = fieldSet.getFieldValue(fieldName, message);
				if (fieldValue == null) fieldValue = "";
				
				fieldTitles.add(fieldName);
				fieldValues.add(fieldValue);
				fieldViews.add(fieldItem.description());
				keyValueMap.put(fieldName, fieldValue);
				


				// need to pass through xml content
                if (fieldValue.contains("<")) fieldValue = fieldValue.replaceAll("<", "&lt;");
                if (fieldValue.contains(">")) fieldValue = fieldValue.replaceAll(">", "&gt;");
				
				String fieldName2 = convertToLink(fieldName, fieldItem.funct());
				result.append(" ").append(fieldName2).append("=").append(fieldValue);
			} catch(Throwable t) {
				LOGGER.warn("Field:" + fieldName, t);
			}
		}
		return result.toString();
	}
	public String populateFieldValuesAsBasic(Set<String> ignoreTheseFieldNames, FieldSet fieldSet) {
		StringBuilder result = new StringBuilder();
		fieldTitles = new CopyOnWriteArrayList<String>();
		fieldValues = new CopyOnWriteArrayList<String>();
		fieldViews = new CopyOnWriteArrayList<String>();

		addDefautFields(fieldSet);
		
		fieldTitles.add(FieldSet.DEF_FIELDS._host.name());
		fieldValues.add(defaultFieldValues[FieldSet.DEF_FIELDS._host.ordinal()]);
		fieldViews.add("");
		
		fieldTitles.add("_raw_");
		fieldValues.add(rawLineData);
		fieldViews.add("");
		
		fieldTitles.add(FieldSet.DEF_FIELDS._type.name());
		fieldValues.add(defaultFieldValues[FieldSet.DEF_FIELDS._type.ordinal()]);
		fieldViews.add("");
		
		fieldTitles.add(FieldSet.DEF_FIELDS._filename.name());
		fieldValues.add(defaultFieldValues[FieldSet.DEF_FIELDS._filename.ordinal()]);
		fieldViews.add("");
		
		fieldTitles.add(FieldSet.DEF_FIELDS._path.name());
		fieldValues.add(defaultFieldValues[FieldSet.DEF_FIELDS._path.ordinal()]);
		fieldViews.add("");
		
		fieldTitles.add(FieldSet.DEF_FIELDS._tag.name());
		fieldValues.add(defaultFieldValues[FieldSet.DEF_FIELDS._tag.ordinal()]);
		fieldViews.add("");
		
		
		fieldTitles.add(FieldSet.DEF_FIELDS._agent.name());
		fieldValues.add(defaultFieldValues[FieldSet.DEF_FIELDS._agent.ordinal()]);
		fieldViews.add("");
		
		
		return result.toString();
	}

	
	private String convertToLink(String fieldName, String function) {
		if (function == null || function.trim().length() == 0) return fieldName;
		//msg = String.format("%s<b><a href='event:%s'>%s</a></b>%s", msg.substring(0, offset),searchName, group, msg.substring(end, msg.length()));
		return "<a href='event:_field:"+fieldName+":"+this.querySourceIndex+":"+function+"'>["+fieldName+"]</a>";
		//return String.format("<a href='event:_field:%s:%d:%s'>[%s]</a>",fieldName, this.querySourceIndex, function, fieldName);
	}

	public void setDefaultFieldValues(String type, String host, String filename, String path, String tag, String agentType, String url, String data) {
		this.defaultFieldValues = new String[] { type, host, filename, path, tag, agentType, url ,data};
	}

	private String getMsgData(FieldSet fieldSet, boolean includeFieldName, boolean delimit) {
		
		String[] message = fieldSet.getFields(rawLineData, -1, lineNumber, this.id.timeMs);
		
//		if (message == null) return null;
		if (fieldSet == null) {
			return com.liquidlabs.common.StringUtil.removeControlChars(Arrays.toString(message));
		}
		StringBuilder result = new StringBuilder();

		if (defaultFieldValues != null) addDefautFields(fieldSet);
		
		for (FieldI fieldItem : fieldSet.fields()) {
			
			if (fieldItem.isVisible() == false) continue;
			
			String fieldName = fieldItem.name();
			
			if (!delimit && fieldName.equals(FieldSet.DEF_FIELDS._filename.name())) continue;
			if (!delimit && fieldName.equals(FieldSet.DEF_FIELDS._host.name())) continue;
			if (!delimit && fieldName.equals(FieldSet.DEF_FIELDS._path.name())) continue;
			if (!delimit && fieldName.equals(FieldSet.DEF_FIELDS._type.name())) continue;
			if (!delimit && fieldName.equals(FieldSet.DEF_FIELDS._tag.name())) continue;
			if (!delimit && fieldName.equals(FieldSet.DEF_FIELDS._agent.name())) continue;
			
			try {
				String fieldValue = fieldSet.getFieldValue(fieldName, message);
				if (fieldValue == null) fieldValue = "";
				
				// need to pass through xml content
                if (fieldValue.contains("<")) fieldValue = fieldValue.replaceAll("<", "&lt;");
				if (fieldValue.contains(">")) fieldValue = fieldValue.replaceAll(">", "&gt;");
				
				if (includeFieldName) {
					result.append(fieldName).append("=").append(fieldValue).append(", ");
				} else {
					if (delimit) {
						if (result.length() > 0) result.append(",");
						result.append(fieldValue.replaceAll(",", "."));
					}else result.append(" ").append(fieldValue);
				}
			} catch(Throwable t) {
				LOGGER.warn("getMsgData() :" + fieldItem.toString(), t);
			}
		}
		return com.liquidlabs.common.StringUtil.removeControlChars(result.toString());
	}

	public List<String> getXMLSafeFieldValues() {
		
		ArrayList<String> result = new ArrayList<String>();
		
		for (String fieldValue : this.fieldValues) {
			if (fieldValue == null) result.add(null);
			else {
                if (fieldValue.contains("<")) fieldValue = fieldValue.replaceAll("<","&lt;");
                result.add(fieldValue);
            }
		}
		return result;
	}

	public ReplayEvent stub() {
		return new ReplayEvent("sourceURI",0, 0, 0, "subscriber", 0, "raw line data");
	}

	public String getCsv(FieldSet fieldSet) {
		String file = "";
		String hostname = "";
		String _tag = "";
		String _agent = "";
		String[] message = fieldSet.getFields(rawLineData, -1, lineNumber, this.id.timeMs);
		
		if (fieldSet != null){
			addDefautFields(fieldSet);
			
			file = fieldSet.getFieldValue(FieldSet.DEF_FIELDS._filename.name(), message);
			hostname = fieldSet.getFieldValue(FieldSet.DEF_FIELDS._host.name(), message);
			_tag = fieldSet.getFieldValue(FieldSet.DEF_FIELDS._tag.name(), message);
			_agent = fieldSet.getFieldValue(FieldSet.DEF_FIELDS._agent.name(), message);
		}
		if (file == null) file = "";
		if (hostname == null) hostname = "";
		if (_tag == null) _agent = "";
		if (_agent == null) _agent = "";
		
		String msg = "";
		if (fieldSet != null && fieldSet.id.equals("basic")) {
			msg = hostname + "\t" + file + ":" + lineNumber + "\t";
			msg += Arrays.toString(message);
			msg = msg.replaceAll(",", " ");
		} else {
			msg = getMsgData(fieldSet, false, true);
		}
		msg = StringEscapeUtils.escapeHtml4(msg);
		
		int limit = 60;
		if (file.length() > limit) file = "..." + file.substring(file.length() - limit);
		
		// cannot handle tab chars in jasper print report because it blows up with paragraphutil.getFirstTabStop Array out of bounds error...
		// 
		msg = msg.replaceAll("\t", " ");

//		return String.format("%s,%s,%s,%d%s\r\n", hostname, file, DateUtil.shortDateTimeFormat3.print(time), lineNumber, msg);
		return msg + "\r\n";//tring.format("%s,%s,%s,%d%s\r\n", hostname, file, DateUtil.shortDateTimeFormat3.print(time), lineNumber, msg);
	}

	private void addDefautFields(FieldSet fieldSet) {
		if (defaultFieldValues != null) fieldSet.addDefaultFields(defaultFieldValues[FieldSet.DEF_FIELDS._type.ordinal()], 
									defaultFieldValues[FieldSet.DEF_FIELDS._host.ordinal()], 
									defaultFieldValues[FieldSet.DEF_FIELDS._filename.ordinal()], 
									defaultFieldValues[FieldSet.DEF_FIELDS._path.ordinal()], 
									defaultFieldValues[FieldSet.DEF_FIELDS._tag.ordinal()],
									defaultFieldValues[FieldSet.DEF_FIELDS._agent.ordinal()],
                                    defaultFieldValues[DEF_FIELDS._sourceUrl.ordinal()],
                                    Long.parseLong(defaultFieldValues[DEF_FIELDS._size.ordinal()]),
									true);
	}

	public String getCsvHeader(FieldSet fieldSet) {
		StringBuilder result = new StringBuilder();
		Set<String> fields = new HashSet<String>();
		
		if (fieldSet != null) {
			int pos = 0;
			for (FieldI fieldItem : fieldSet.fields()) {
				if (fieldItem.isVisible()) {
					if (result.length() > 0) result.append(",");
					String fieldName = fieldItem.name();
					if (fields.contains(fieldName)) fieldName += pos++;
					result.append(fieldName);
					fields.add(fieldName);
				}
			}
		}
		result.append("\r\n");

		return result.toString();
	}

	public void populateFieldValues(Set<String> ignoreTheseFieldNames, List<FieldSet> fieldSets) {
        String fieldSetId = getDefaultField(DEF_FIELDS._type);
		for (FieldSet fieldSet : fieldSets) {
			if (fieldSet.getId().equals(fieldSetId)) {
				populateFieldValues(ignoreTheseFieldNames, fieldSet);
			}
		}
	}

    public String getDefaultField(String name) {
        return getDefaultField(DEF_FIELDS.valueOf(name));
    }
	public String getDefaultField(DEF_FIELDS item) {
		if (this.defaultFieldValues == null || this.defaultFieldValues.length == item.ordinal()) return "unknown";
		return this.defaultFieldValues[item.ordinal()];
	}
    public String[] getDefaultFields() {
        return this.defaultFieldValues;
    }

	public void setRawData(String rawLineData2) {
		this.rawLineData = rawLineData2;
	}
	
	public int hashCode() {
		return this.id.hashCode();
	}
	public boolean equals(Object obj) {
		if (!(obj instanceof ReplayEvent)) return false;
		ReplayEvent other = (ReplayEvent) obj;
		
		return this.id.equals(other.getId());
	}

	public void setId(TimeUID newUid) {
		this.id = newUid;
	}

	public void setDefaultField(FieldSet.DEF_FIELDS item, String value) {
		defaultFieldValues[item.ordinal()] = value;
	}

	public void setSourceURI(String value) {
		this.sourceURI = value;
	}


	public void setLineNumber(int i) {
		this.lineNumber = i;
	}

    public List<String> getFieldNames(FieldSet fieldSet, boolean includeDynamic) {
        fieldSet.addDefaultFields(getDefaultFields());
        return fieldSet.getFieldNames(true, true, includeDynamic, false, true);
    }
    public String toJsonEventView(FieldSet fieldSet, String baseUrl) {
        fieldSet.addDefaultFields(getDefaultFields());
        String[] fields = fieldSet.getFields(rawLineData, -1, lineNumber, this.id.timeMs);

        List<String> fieldNames = fieldSet.getFieldNames(true, true, true, false, true);
        String results = "{";
        for (String fieldName : fieldNames) {

            String value = JSONObject.quote(getFV(fieldSet, fields, fieldName));
            value = value.substring(1, value.length()-1);
			results += "\"" + fieldName + "\": \"" + value + "\"\n,";
        }
		results += "\"_filename_tail\": " + JSONObject.quote(getHtmlFilenameTailLink(baseUrl)) + ",\n";
		results += "\"_filename_dll\": " + JSONObject.quote(getHtmlFilenameDLLink(baseUrl))+ ",\n";
		results += "\"_lineNumber\": " + lineNumber;


		return results + "}\n";
    }
    public String toJson(int rowNum, String filter, FieldSet fieldSet, ReplayEvent.Mode mode, String baseUrl, Matcher filterMatcher) {

        // regexp based filter...
        if (filterMatcher != null) {
            if (!filterMatcher.matches(rawLineData) && !filterMatcher.matches(getHostname()) && !filterMatcher.matches(getFilePath())) return null;

        } else {
            if (filter != null && !isSubstringMatch(filter)) return null;
        }


        // not perfect but it will give matching a good go...

        String timeAsString = DateUtil.shortTimeFormatter4.print(id.timeMs);
        if (mode == Mode.raw) {
            String content = rawLineData;
            content = makeHtmlGood(content, baseUrl);
            content =  JSONObject.quote(content);
                    return "{\"DT_RowClass\":\"\",\n\t"+
                    "\"time\": \"" + timeAsString + "\",\n\t" +
                    "\"msg\":  " + content + ",\n\t" +
                    "\"_host\": " + makeClickable(DEF_FIELDS._host.name(), getHostname()) + ",\n\t" +
                    "\"_tag\": " + makeClickable(DEF_FIELDS._tag.name(), getDefaultField(DEF_FIELDS._tag)) + ",\n\t" +
                    "\"_type\": " + makeClickable(DEF_FIELDS._type.name(), getDefaultField(DEF_FIELDS._type))  + "}\n";
        } if (mode == Mode.structured) {
			String results = "{\"DT_RowClass\":\"\",\n\t";

			results += "\"time\": \"" + timeAsString + "\",\n\t" +
					"\"events\":  " + makeStructuredJson(fieldSet, baseUrl) + ",\n\t" +
					"\"_host\": " + makeClickable(DEF_FIELDS._host.name(), getHostname()) + ",\n\t" +
					"\"_tag\": " + makeClickable(DEF_FIELDS._tag.name(), getDefaultField(DEF_FIELDS._tag)) + ",\n\t" +
					"\"_type\": " + makeClickable(DEF_FIELDS._type.name(), getDefaultField(DEF_FIELDS._type))  + "}\n";

			return  results;


		} else {
           String results = "{\"DT_RowClass\":\"\",\n\t";

            results +=     "\"time\": \"" + timeAsString + "\"";

            fieldSet.addDefaultFields(getDefaultFields());
            String[] fields = fieldSet.getFields(rawLineData, -1, lineNumber, this.id.timeMs);

            List<String> fieldNames = fieldSet.getFieldNames(true, true, true, false, true);

            for (String fieldName : fieldNames) {
                results += ",\n\t";
                String value = JSONObject.quote(getFV(fieldSet, fields, fieldName));
                value = value.substring(1, value.length()-1);
                value = "\"<span class='word_split' field='"+fieldName +"'>" + value + "</span>\"";

                if (fieldName.equals("_filename")) {
                    // attach some download links
                    results += "\"" + fieldName + "\": " + JSONObject.quote(getHtmlFilename(baseUrl)) + "";

                } else {
                    results += "\"" + fieldName + "\": " + value + "";
                }
            }
            return results + "}\n";

        }

    }

	public boolean isSubstringMatch(String filter) {
		return filter.length() == 0 ||
				StringUtil.containsIgnoreCase(rawLineData, filter) ||
                StringUtil.containsIgnoreCase(getHostname(), filter) ||
                StringUtil.containsIgnoreCase(getFilePath(), filter);
	}

	private String makeStructuredJson(FieldSet fieldSet, String baseUrl) {
		fieldSet.addDefaultFields(getDefaultFields());
		String[] fields = fieldSet.getFields(rawLineData, -1, lineNumber, this.id.timeMs);
		List<String> fieldNames = fieldSet.getFieldNames(true, true, true, false, true);
		String results = " \"";
        results += "<a href='#' class='structEvtEdit fa fa-plus-circle no-link-uline' data-field='" + this.getId() + "'  data-filename='" + this.getFilename() + "'></a>";
		for (String fieldName : fieldNames) {
			if (fieldName.equals("_size")) continue;
			String value = JSONObject.quote(getFV(fieldSet, fields, fieldName));
			value = value.substring(1, value.length()-1);
			String fieldType = getFieldType(fieldName, fieldSet);

			if (value != null && value.length() > 0) {
                String[] tokenised = value.split(" ");
                for (String val : tokenised) {
                    results += "<span class='word_split field_evt " + fieldType + "' field='" + fieldName + "' title='Field:" + fieldName + " (" + fieldType.replace("field_","") + ")'>" + val + "</span>";
                }

			}
		}

		return results + "\" ";
	}

	private String getFieldType(String fieldName, FieldSet fieldSet) {
		String result= "field_synth";

		if (FieldSet.isDefaultField(fieldName)) {
			result = "field_sys";
		} else if (fieldSet.isDiscoveredField(fieldName)) {
			result = "field_disco";
		} else if (fieldSet.isStandardField(fieldName)) {
			result= "field_std";
		} else if (fieldSet.getField(fieldName) instanceof GroupField) {
			result = "field_group";
		}
		return result;
	}

	private String makeClickable(String name, String value) {
        return "\"<span class='word_split' field='"+name +"'>" + value + "</span>\"";
    }

    private String getFV(FieldSet fieldSet, String[] fields, String fieldName) {
        try {
            if (FieldSet.isDefaultField(fieldName)) {
                return this.getDefaultField(fieldName);
            }
            if (this.getDefaultField("_type").equals(fieldSet.getId())) {
                String value = fieldSet.getFieldValue(fieldName, fields);
                return StringEscapeUtils.escapeHtml4(value);
            }
            return fieldSet.getFieldValue(fieldName, fields);
        } catch (Throwable t) {
            return "";
        }

    }
	private String getHtmlFilenameDLLink(String baseUrl) {
		String filePath = getFilePath();
		String url1 = baseUrl  + this.sourceURI + filePath;
		return url1.replaceAll("\\\\","/");
	}
	private String getHtmlFilenameTailLink(String baseUrl) {
		String filePath = getFilePath();
		String url1 = baseUrl  + this.sourceURI + filePath;
		url1 = url1.replaceAll("\\\\","/");
		return url1 + ".html?from="+ lineNumber;
	}

	private String getHtmlFilename(String baseUrl) {
        String filePath = getFilePath();
        String url1 = baseUrl  + this.sourceURI + filePath;
        url1 = url1.replaceAll("\\\\", "/");
        String url2 = url1 + ".html?from="+ lineNumber;

        String links = "<a title='Opens in Explore' target='_blank' linenumber='" + lineNumber +
                "' filename='" + this.getFilename() +
                "' source='" + this.getSourceURI() +
                "' host='" + this.getHostname() +
                "' path='" + this.getFilePath() + "' href='"+url2 + "'>" + this.getFilename() + ":" + lineNumber + "</a> "  +
                "<a  title='Download' href='"+url1 + "' /><i class='fa fa-download'></i></p>";
        return links;
    }

    private String makeHtmlGood(String content, String baseUrl) {
        String filePath = getFilePath();
        if (filePath.indexOf('/') != 0) filePath = '/' + filePath;

        String url1 = baseUrl  + this.sourceURI + filePath;
        url1 = url1.replaceAll("\\\\","/");
        String url2 = url1 + ".html?from="+ lineNumber;


        String links = "<a title='Opens in Explore' target='_blank' linenumber='" + lineNumber +
                "' filename='" + this.getFilename() +
                "' source='" + this.getSourceURI() +
                "' host='" + this.getHostname() +
                "' path='" + this.getFilePath() +
                "' href='"+url2 + "'>" + this.getFilename() + ":" + lineNumber + "</a> "  +
                "<a  title='Download' href='"+url1 + "' /><i class='fa fa-download'></i></p>";

        String htmlContent = "<p class='word_split'><span>";
        htmlContent     += StringEscapeUtils.escapeHtml4(content).replaceAll(" ", "</span> <span>");
        htmlContent     = htmlContent.replaceAll("\t","</span>\t<span>");
        htmlContent     = htmlContent.replaceAll(":","</span>:<span>");
        htmlContent     = htmlContent.replaceAll(",","</span>,<span>");
        if (htmlContent.contains("\n"))  htmlContent     = htmlContent.replaceAll("\n","</span>\n<span>");
        else if (htmlContent.contains("\r")) htmlContent  = htmlContent.replaceAll("\r","</span>\n<span>");
        htmlContent     += "</span>";
        return htmlContent + "<br>" + links;
    }
    public void read(Kryo kryo, Input in) {
        this.id =  kryo.readObject(in, TimeUID.class);
        this.subscriber = kryo.readObject(in, String.class);
        this.groupIndex =  kryo.readObject(in, short.class);
        this.querySourceIndex = kryo.readObject(in, short.class);
        this.sourceURI = kryo.readObject(in, String.class);
        this.rawLineData = kryo.readObject(in, String.class);
        this.lineNumber = kryo.readObject(in, int.class);
        this.defaultFieldValues = kryo.readObject(in, String[].class);
    }

    public void write(Kryo kryo, Output out) {
        kryo.writeObject(out, this.id);
        kryo.writeObject(out, this.subscriber);
        kryo.writeObject(out, this.groupIndex);
        kryo.writeObject(out, this.querySourceIndex);
        kryo.writeObject(out, this.sourceURI);
        kryo.writeObject(out, this.rawLineData);
        kryo.writeObject(out, this.lineNumber);
        kryo.writeObject(out, this.defaultFieldValues);
    }

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeLong(this.id.timeMs);
		out.writeUTF(this.id.uid);
		out.writeUTF(this.subscriber);
		out.writeShort(this.groupIndex);
		out.writeShort(this.querySourceIndex);
		out.writeUTF(sourceURI);
		out.writeUTF(rawLineData);
		out.writeInt(lineNumber);
		out.writeShort(this.defaultFieldValues.length);
		for (String defaultFieldValue : defaultFieldValues) {
			out.writeUTF(defaultFieldValue);
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.id = new TimeUID(in.readLong(), in.readUTF());
		this.subscriber = in.readUTF();
		this.groupIndex = in.readShort();
		this.querySourceIndex = in.readShort();
		this.sourceURI = in.readUTF();
		this.rawLineData = in.readUTF();
		this.lineNumber = in.readInt();
//		this.defaultFieldValues = (String[]) in.readObject();
		short length = in.readShort();
		this.defaultFieldValues = new String[length];
		for (int i = 0; i < length; i++) {
			this.defaultFieldValues[i] = in.readUTF();
		}
	}



    public static class EventSerializer implements Serializer<ReplayEvent>, Serializable{

        @Override
        public void serialize(DataOutput out, ReplayEvent value) throws IOException {
            // Ignore Id,subsrcriber,groupIndex,querySourceIndex,
            out.writeUTF(value.getSourceURI());
            out.writeLong(value.getTime());
            out.writeUTF(value.getRawData());
            out.writeInt(value.getLineNumber());
            out.writeInt(value.getDefaultFields().length);
            String[] defaultFields = value.getDefaultFields();
            for (String defaultField : defaultFields) {
                out.writeUTF(defaultField);
            }
        }

        @Override
        public int fixedSize() {
            return -1;
        }

        @Override
        public ReplayEvent deserialize(DataInput in, int available) throws IOException {
            ReplayEvent event = new ReplayEvent();
            try {
                event.setSourceURI(in.readUTF());
                event.setRawData(in.readUTF());
                event.setLineNumber(in.readInt());
                int dfLength = in.readInt();
                String[] dFields = new String[dfLength];
                for (int i = 0; i < dfLength; i++) {
                    dFields[i] = in.readUTF();
                }
                event.defaultFieldValues = dFields;
            } catch (Throwable t) {
                LOGGER.error("Failed to DESER REPLAY",t);
            }
            return event;
        }
    }


    public void cacheValues(Map<String, String> cachedStrings) {
        // save memory
        this.setSubscriber(cache(this.subscriber(), cachedStrings));
        this.setDefaultField(DEF_FIELDS._host, cache(this.getDefaultField(DEF_FIELDS._host), cachedStrings));
        this.setDefaultField(DEF_FIELDS._filename, cache(this.getDefaultField(DEF_FIELDS._filename), cachedStrings));
        this.setDefaultField(DEF_FIELDS._tag,cache(this.getDefaultField(DEF_FIELDS._tag), cachedStrings));
        this.setDefaultField(DEF_FIELDS._agent, cache(this.getDefaultField(DEF_FIELDS._agent), cachedStrings));
        this.setDefaultField(DEF_FIELDS._type, cache(this.getDefaultField(DEF_FIELDS._type), cachedStrings));
        this.setDefaultField(DEF_FIELDS._path, cache(this.getDefaultField(DEF_FIELDS._path), cachedStrings));
        this.setDefaultField(DEF_FIELDS._sourceUrl, cache(this.getDefaultField(DEF_FIELDS._sourceUrl), cachedStrings));
        this.setSourceURI(cache(getSourceURI(), cachedStrings));
    }
    private String cache(String item, Map<String, String> cache) {
        if (item == null) return null;
        String found = cache.get(item);
        if (found == null) {
            cache.put(item, item);
            found = item;
        }
        return found;
    }




}
