package com.liquidlabs.log.fields;

import com.google.common.base.Splitter;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.regex.RegExpUtil;
import com.liquidlabs.log.fields.field.*;
import com.logscape.disco.indexer.IndexFeed;
import com.logscape.disco.indexer.KvIndexFactory;
import com.logscape.disco.indexer.Pair;
import com.logscape.disco.kv.KVExtractor;
import com.logscape.disco.kv.RulesKeyValueExtractor;
import com.logscape.disco.grokit.GrokItPool;
import com.liquidlabs.orm.Id;
import com.liquidlabs.transport.serialization.Convertor;
import com.liquidlabs.vso.agent.metrics.DefaultOSGetter;
import groovy.lang.MissingPropertyException;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ThreadFactory;

/**
 * Describes MetaData about a logSource event structure = fieldnames, timestamp
 * fields etc
 *
 * @author Neil
 *
 */
public class FieldSet implements Serializable {


	public static final String JREX = "jrex";
	public static final String JAVA = "java";
	public static final String ORO = "oro";
	private static final String COMMA = ",";
    public static final Object BASIC = "basic";

	private static final Logger LOGGER = Logger.getLogger(FieldSet.class);
    transient private boolean isDiscoveryEnabled = true;


    public KVExtractor getKeyValueExtractor() {
        return kve;
    }

    public void reset() {
        if (dynamicFields != null) dynamicFields.clear();
        if (fieldCacheMap != null) fieldCacheMap.clear();
        if (fieldValueCache != null) fieldValueCache.clear();
        if (defaultFieldsCache != null) defaultFieldsCache.clear();
        if (context != null) context.clear();
        if (extractor != null) extractor.reset();
    }

    public boolean containsField(String applyToField) {
        return allFieldNamesCacheSet.contains(applyToField) || dynamicFields.containsKey(applyToField);
    }

    public boolean validate() {
        return !(id.length() == 0 || id.contains(" "));
    }

    public void enhance() {

        String type = ((LiteralField)getField(DEF_FIELDS._type.name())).getValue();
        String filename = ((LiteralField)getField(DEF_FIELDS._filename.name())).getValue();
        if (type.equals("basic") && filename.endsWith(".csv")) EnhancerCSV.enhance(this);


    }

    public List<String> getFieldNamesUsingWCard(String fieldname) {
        Set<String> strings = this.dynamicFields.keySet();
        List<String> results = new ArrayList<>();
        String cleanField = fieldname.replace("*","");
        for (String  thisField : strings) {
            if (thisField.contains(cleanField)) {
                results.add(thisField);
            }
        }
        return results;
    }

    public boolean isMultiField(String field) {
        return field.contains("*") || field.contains("[");
    }

    public List<String> getMultiFields(String field) {
        if (field.contains("*")) return getFieldNamesUsingWCard(field);
        if (field.contains("[]")) return getFieldNamesForArray(field);

        return new ArrayList<String>();
    }

    private List<String> getFieldNamesForArray(String field) {
        List<String> results = new ArrayList<>();
        String[] split = field.split("\\[\\]");
        Set<String> dynFields = this.dynamicFields.keySet();
        for (String dynField : dynFields) {
            if (dynField.startsWith(split[0])) {
                int hits = 0;
                for (String part : split) {
                    if (dynField.contains(part)) hits++;
                }
                if (hits == split.length) results.add(dynField);
            }
        }

        return results;
    }

    public void setDiscoFields(Map<String, String> discoFields) {
        this.dynamicFields = discoFields;
    }


    enum SCHEMA { example, expression,fields,fileNameMask,filePathMask,id,priority,timeStampField,lastModified };

	@Id
	public String id = "";

	// expression to check the matching against a directory - i.e. /var/log* -
	// could be commad delimited - defaults to *
	public String filePathMask = "*";
	// expression to check the matching against a filemask i.e. system.log -
	// could be comma delimited list of fileNameMasks, defaults to *
	public String fileNameMask = "*";

	// used to determine precedence of application when adding watch files
	public int priority;

	public String expression;
	public int timeStampField;
	
	public long lastModified = 0;

	public List<FieldI> fields = new ArrayList<FieldI>();

	public String[] example;

    transient private ArrayList<String> errors;

    public List<FieldI> fields() {
        return this.fields;
    }
    public String exampleText() {
        StringBuilder sb = new StringBuilder();
        if (example == null) example = new String[0];
        for (String s : example) {
            sb.append(s).append("\n\n");
        }
        String result = sb.toString();
        while (result.contains("\n\n\n")) result = result.replaceAll("\n\n\n","\n\n");
        return result;
    }


    public FieldSet() {
	}

    public FieldSet(String expression, String... fieldNames) {
        this.expression = expression;
        int pos = 1;
        for (String field : fieldNames) {
            addField(field, "count()",true, pos++, true, "","", "", false);
        }
        lastModified = System.currentTimeMillis();
    }
	public FieldSet(String name, String[] example, String expression, String filemask, int precedence) {
		this.id = name;
		this.example = example;
		expression = expression.replaceAll("\\\\t", "\t");
		this.expression = expression;
		this.fileNameMask = filemask;
		this.priority = precedence;
        lastModified = System.currentTimeMillis();

	}

	final public String[] getFields(String nextLine, int logId, int lineNumber, long timeMs) {
        String[] results = getNormalFields(nextLine);
        buildDiscoveredFields(logId, lineNumber, nextLine, timeMs);
        return results;
	}
    final public String[] getFields(String nextLine) {
        return getFields(nextLine, -1, -1, -1);
    }

	transient String reEngine = System.getProperty("re.engine", JREX);
    transient Extractor extractor;
    transient String line;

	final public String[] getNormalFields(final String nextLine) {
		if (nextLine == null) return com.liquidlabs.common.collection.Arrays.EMPTY_ARRAY;
        line = nextLine;
        fieldValueCache.clear();
        selectedFieldNamesCache.clear();
        context.clear();
		if (nextLine.length() == 0) {
            if (extractor != null) extractor.reset();;
            return com.liquidlabs.common.collection.Arrays.EMPTY_ARRAY;
        }
        if (expression.equals("(**)") || expression.equals("*")) return new String[] { nextLine };
        return getExtractor(expression).extract(nextLine);
	}
    private Extractor getExtractor(String expression) {
        if (extractor != null) return extractor;
        if (expression.startsWith("split(")) {
            FieldI field = getField(DEF_FIELDS._filename.name());

            String filename = field != null ? ((LiteralField) field).getValue() : "";
            if(filename.endsWith(".csv")) {
                extractor = new CsvSplitUtil();
            }   else {
                extractor = new StringSplitExtractor(expression);
            }
        } else if (reEngine.equals(JREX)) {
            extractor = new JRexExtractor(expression);
        } else if (reEngine.equals(ORO)) {
            extractor = new OroExtractor(expression);
        } else {
            extractor = new JRexExtractor(expression);
        }
        return extractor;
    }


    public LinkedHashMap<String, String> getFieldValues(String raw, int lineNumber, int logId, long timeMs) {
        context.clear();
        String[] events = getFields(raw, logId, lineNumber, timeMs);
        LinkedHashMap<String, String> results = new LinkedHashMap<String, String>();

        for (int i = 0; i < fields.size(); i++) {
            FieldI field =  fields.get(i);
            String fieldValue = getFieldValue(field.name(), events);
            results.put(field.name(), fieldValue);
        }
        return results;
    }
	public transient Map<String, FieldI> fieldCacheMap = new HashMap<String, FieldI>();
	transient Map<String, String> fieldValueCache = new HashMap<String, String>();


    public List<Pair> getIndexedFields(String[] events) {
        ArrayList<Pair> pairs = new ArrayList<>();
        for (FieldI field : fields) {
            if (field.isIndexed()) {
                String value = getFieldValue_INTERNAL(events, field);
                if (value != null) {
                    pairs.add(new Pair(field.name(), value));
                }
            }
        }

        return pairs;
    }

    transient Map<String,String> dynamicFields = new LinkedHashMap<String, String>();

    public Map<String, String> getFieldsAsMap(int logId, String nextLine, int lineNumber, long lineTime, List<Pair> discovered) {
        String[] fields = getNormalFields(nextLine);
        List<String> names = getFieldNames(true, true, false, false, true);
        Map<String, String> allFields = new HashMap<String, String>();
        for (Pair fieldI : discovered) {
            allFields.put(fieldI.key, fieldI.value);
        }

        for (String name : names) {
            if (!getField(name).isIndexed()){
                String fieldValue = getFieldValue(name, fields);
                if (fieldValue != null) {
                    allFields.put(name, fieldValue);
                }
            }
        }
        return  allFields;
    }


    final public String getFieldValue(final String name, final String[] events, final com.liquidlabs.common.regex.MatchResult mr) {
        try {
            if (name.indexOf("+") != -1) {
                return getFieldValueConcat(name.split("\\+"), events, mr);
            }
            if (StringUtil.isIntegerFast(name)) {
                return mr.group(StringUtil.isInteger(name));
            } else {
                return getFieldValue(name, events);
            }
        } catch (Throwable t) {
            LOGGER.warn("getFieldValue:" + this.id + "." + name + " ex:" + t.toString());
            return null;
        }

    }

    final public String getFieldValue(final String name, final String[] events) {

        context = new HashSet<String>();
        if (name == null || name.length() == 0)
            return null;

        if (name.charAt(0) == '_' && defaultFieldsCache.containsKey(name)) {
            String defaultFieldValue = defaultFieldsCache.get(name);
            if (defaultFieldValue != null) return  defaultFieldValue;
        }
        FieldI field = findField(name);
        if (field == null) {
            return  dynamicFields.get(name);
        }
        if (field.isIndexed()) {
            return dynamicFields.get(name);
        }
        return getFieldValue_INTERNAL(events, field);
    }


    private String getFieldValueConcat(String[] split, String[] events, com.liquidlabs.common.regex.MatchResult mr) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String fieldName : split) {
            if (stringBuilder.length() > 0) stringBuilder.append("-");
            stringBuilder.append(getFieldValue(fieldName, events, mr));
        }
        return stringBuilder.toString();
    }


    transient HashSet<String> context = new HashSet<String>();
    private String getFieldValue_INTERNAL(String[] events, FieldI field) {

        try {
            String source = field.synthSource();
            if (source != null && source.length() > 0) {
                String sourceValue = getFieldValue(field.synthSource(), events);
                if (sourceValue == null) return null;
            }
            return field.get(events, context , this);
        } catch (Throwable e) {
            if (errors == null) errors = new ArrayList<>();
            if (errors.size() < 20) {
                if (e instanceof MissingPropertyException) {
                    LOGGER.warn("FieldSet: " + this.id + " errors:" + errors.size() + " Failed to getField:" + field.name() + " ex:" + e.toString() + " To prevent this error: specify the dependent field as the 'sourceField'");
                    addError(field.name(), e, " To prevent this error - specify the missing field as a 'Synth Source'");
                } else {
                    LOGGER.warn("FieldSet: " + this.id + " errors:" + errors.size() + " Failed to getField:" + field.name() + " ex:" + e.toString() + " line:" + this.line + " MissingProp");
                    addError(field.name(), e, "");
                }
            }
            return null;
        }
    }
    transient KVExtractor kve = new RulesKeyValueExtractor();
    transient IndexFeed kvStore;
    transient boolean isDynamicFieldCache = true;
    static GrokItPool grokit = new GrokItPool();

    public void setDiscovered(List<Pair> ff) {
        Map<String, String> fieldMap = new HashMap<String, String>();
        for (Pair fieldI : ff) {
            fieldMap.put(fieldI.key, fieldI.value);
        }
        try {
            fieldValueCache.keySet().removeAll(dynamicFields.keySet());
        } catch (Throwable t){}

        dynamicFields.clear();
        dynamicFields.putAll(fieldMap);
        fieldValueCache.clear();
    }

    private void buildDiscoveredFields(int logId, int lineNumber, String line, long timeMs) {
        if (fieldValueCache == null) fieldValueCache = new HashMap<String, String>();
        fieldValueCache.clear();
        if (!isDiscoveryEnabled) return;
        // LogId != -1 then use the KVIndex-Store
        if (logId != -1 && isDynamicFieldCache) {

            dynamicFields = getKVStore().getAsMap(logId, lineNumber, timeMs);
        } else {
            // Pull out all discovered and grokit values - heavy processing
            Map<String, String> fieldMap = new HashMap<String, String>();
            List<Pair> ff = kve.getFields(line);
            for (Pair fieldI : ff) {
                fieldMap.put(fieldI.key, fieldI.value);
            }
            Map<String, String> stringStringMap = grokit.processLine("", line);
            for (Map.Entry<String, String> key : stringStringMap.entrySet()) {
                fieldMap.put(key.getKey(), key.getValue());
            }

            dynamicFields.putAll(fieldMap);
        }
    }
    public IndexFeed getKVStore() {
        if (kvStore == null) kvStore = KvIndexFactory.get();
        return kvStore;
    }



    public void configureKVE(RulesKeyValueExtractor.Config config) {
        kve = new RulesKeyValueExtractor(config);
    }
    public RulesKeyValueExtractor.Config getKVEConfig() {
        return kve.getConfig();
    }

    private void addError(String name, Throwable t, String msg) {
        if (this.errors == null) this.errors = new ArrayList<String>();
        if (this.errors.size() < 20) this.errors.add("Field:" + name + " " + msg + " error:" + t.getMessage());
    }

    private FieldI findField(String name) {
		FieldI found = fieldCacheMap.get(name);
		if (found != null) return found;
        else if (fieldCacheMap.size() == fields.size()) {
            return null;
        }

		// cycle through the lot so the cacheMap gets populated
		for (FieldI field : fields) {
			fieldCacheMap.put(field.name(), field);
			if (field.name().equals(name)) {
				found = field;
                return found;
			}
		}
		return found;
	}

	final public String getId() {
		return this.id;
	}

	private transient boolean defaultAdded = false;

    public void setIsDiscoveryEnabled(boolean isDiscoveryEnabled) {
        this.isDiscoveryEnabled = isDiscoveryEnabled;
    }

    public Map<String,String> getDynamicFields() {
        return dynamicFields;
    }

    transient HashMap<String, String> defaultFieldsCache = new HashMap<String, String>();
    public enum DEF_FIELDS { _type, _host, _filename, _path, _tag, _agent, _sourceUrl, _size, _timestamp, _datetime};
	public static String[] DEF_DESC = new String[] { "describe(The data-type which this field belongs to)","describe(The host where the data is found)",
								"describe(The filename of the found data)", "describe(The full file path)",
								"describe(The data-source tag of the found data)", "describe(The agent-role where the data was found)", "SourceURL", "data size" };
		
	final public void addDefaultFields(String fieldSetId, String hostname, String filename, String path, String tag, String agent, String _sourceUrl, long _size, boolean overwrite) {
		if (!overwrite && defaultAdded) return;
		this.defaultAdded = true;
		FieldI typeF = setDefaultField(DEF_FIELDS._type, fieldSetId);
		typeF.setDescription(DEF_DESC[DEF_FIELDS._type.ordinal()]);
		
		FieldI typeH = setDefaultField(DEF_FIELDS._host, hostname);
		typeH.setDescription(DEF_DESC[DEF_FIELDS._host.ordinal()]);
		
		FieldI typeFilename = setDefaultField(DEF_FIELDS._filename, filename);
        typeFilename.setDescription(DEF_DESC[DEF_FIELDS._filename.ordinal()]);
		
		FieldI tagg = setDefaultField(DEF_FIELDS._tag, tag);
        tagg.setDescription(DEF_DESC[DEF_FIELDS._tag.ordinal()]);

		FieldI aagent = setDefaultField(DEF_FIELDS._agent, agent);
        aagent.setDescription(DEF_DESC[DEF_FIELDS._agent.ordinal()]);

        FieldI asrcUrl = setDefaultField(DEF_FIELDS._sourceUrl, _sourceUrl);
        asrcUrl.setDescription(DEF_DESC[DEF_FIELDS._sourceUrl.ordinal()]);

        FieldI aSize = setDefaultField(DEF_FIELDS._size, Long.toString(_size));
        aSize.setDescription(DEF_DESC[DEF_FIELDS._size.ordinal()]);

        // dont do a summary for the path
        setDefaultField(DEF_FIELDS._path, path).setSummary(false);
        defaultFieldsCache.put(DEF_FIELDS._path.name(), path);

        buildFieldNameCache();


    }

    public void buildFieldNameCache() {
        List<FieldI> fields1 = this.fields;
        for (FieldI fieldI : fields1) {
            this.allFieldNamesCacheSet.add(fieldI.name());
        }
    }

    public FieldI setDefaultField(DEF_FIELDS field, String value) {
        defaultFieldsCache.put(field.name(), value);
        return addOrUpdate(field.name(), value, true);
    }

	private FieldI addOrUpdate(String name, String literal, boolean overwrite) {
        FieldI fieldI = fieldCacheMap.get(name);
        if (fieldI != null) {
            if (overwrite) fieldI.setValue(literal, "count()");
            return fieldI;
        }
        FieldI field = getField(name);
		if (field  != null) {
            fieldCacheMap.put(name, field);
			if (overwrite) field.setValue(literal, "count()");
			return field;
		}

		field = new LiteralField(name,1,true,true, literal, "count()");
		fields.add(field);
		return field;
	}

	public List<String> allErrors() {
        if (errors == null) return new ArrayList<String>();
		return errors;
	}

    // FIX name cache
    transient Set<String> allFieldNamesCacheSet = new HashSet<String>();

    // Currently selected set of fields
    transient List<String> selectedFieldNamesCache = new ArrayList<String>();

    transient List<String> sysField = new ArrayList<String>();
    public List<String> getFieldNames(boolean summary, boolean visible, boolean dynamic, boolean ignoreStandardFields, boolean systemFields) {
        if (!ignoreStandardFields) {
            if (selectedFieldNamesCache.size() == 0) {
                selectedFieldNamesCache = getStandardFieldNames(summary, visible);
            }
        } else {
            if (sysField.size() == 0)  getStandardFieldNames(summary, visible);
            if (selectedFieldNamesCache.size() > 0) selectedFieldNamesCache.clear();
        }
        List<String> results =  new ArrayList<String>(selectedFieldNamesCache);
        if (dynamic) results.addAll(dynamicFields.keySet());
        if (systemFields) results.addAll(sysField);
        return results;
    }
    public List<String> getStandardFieldNames(boolean summary, boolean visible) {
        List<String> results = new ArrayList<String>();
        sysField.clear();
        for (int i = 0; i < fields.size(); i++) {
            FieldI field =  fields.get(i);
            if (field.isSummary() && summary || field.isVisible() && visible) {
                if (field.name().startsWith("_")) sysField.add(field.name());
                else results.add(field.name());
            }

//            if (field.name().startsWith("_") && field instanceof LiteralField) {
//                LiteralField ff = (LiteralField) field;
//                fieldValueCache.put(field.name(),ff.getValue());
//            }
        }
        return results;
    }

	public boolean isPathMatch(String dir) {
        return FileUtil.isPathMatch(true, filePathMask, dir);
	}

	boolean isFilenameMatch(String name) {
		String[] fileMasks = RegExpUtil.getCommaStringAsRegExp(this.fileNameMask, DefaultOSGetter.isA());
		return RegExpUtil.isMatch(FileUtil.getFileNameOnly(name), fileMasks);
	}

	public int hashCode() {
		return id.hashCode();
	}

	public boolean equals(Object obj) {
		return id.equals(((FieldSet) obj).id);
	}

	public String toString() {
		return super.toString() + " id:" + this.id + " file:" + fileNameMask + " p:" + priority;
	}
    public void addSynthField(String name, String src, String expr, String function, boolean visible, boolean summary) {
        addField(name, function, visible, 1, summary, src, expr, "", false);
    }
    public void addSynthField(String name, String src, String expr, String function, boolean visible, boolean summary, int groupId) {
        addField(name, function, visible, groupId, summary, src, expr, "", false);
    }

    public void addField(String name, String function, boolean visible, boolean summary) {
        addField(name, function, visible, this.fields.size()+1, summary, "","", "", false);
    }


    public void addField(String name, String function, boolean visible, int groupId, boolean summary, String synthSrc, String synthExpr, String desc, boolean indexed) {
        FieldI field = getField(name);
        if (field == null) {
            field = FieldFactory.getField(name, synthExpr, synthSrc, groupId, visible, summary, function, indexed);
            field.setDescription(desc);
            field.setIndexed(indexed);
            fields.add(field);
        }
    }

    transient Map<String, FieldI> fieldsCacheMap = new HashMap<String, FieldI>();
	public FieldI getField(String fieldName) {
		if (fieldName == null) return null;

        FieldI fieldI = fieldsCacheMap.get(fieldName);
        if (fieldI != null) return fieldI;

        for (FieldI field : fields) {
           if (field.name().equals(fieldName)) {
               fieldsCacheMap.put(fieldName, field);
               return field;
           }
        }
        // check dynamic Fields;

        String dynamicField = dynamicFields != null ?   dynamicFields.get(fieldName) : null;
        if (dynamicField != null) {
            return new LiteralField(fieldName,-1,true, true, dynamicField,"");
        } else {
            return null;
        }
	}

	public List<FieldI> getFields() {
		return this.fields;
	}

    public void addDefaultFields(String[] defaultFieldValues) {
        for (int i = 0; i < defaultFieldValues.length; i++) {
            addOrUpdate(DEF_FIELDS.values()[i].name(), defaultFieldValues[i], true);
        }
    }
    public String getDefaultFieldValue(DEF_FIELDS defaultFieldValue) {
        LiteralField field = (LiteralField) getField(defaultFieldValue.name());
        return field.getValue();
    }


    public boolean matchesFilenameAndPath(String fileName) {
		return this.isPathMatch(FileUtil.getParent(fileName)) && this.isFilenameMatch( FileUtil.getFileNameOnly(fileName));
	}
    public boolean matchesFilenameAndTAG(String fileName, String tag) {
        return this.matchesFileTag(tag) && this.isFilenameMatch( FileUtil.getFileNameOnly(fileName));
    }
    public boolean matchesFileTag(String tags) {
        if (tags == null || tags.length() == 0) return false;
        Iterable<String> file = Splitter.on(',').split(tags);
        Iterable<String> pathTags = Splitter.on(',').split(filePathMask);
        for (String pathTag : pathTags) {
            pathTag = pathTag.trim();
            if (pathTag.startsWith("tag:")) {
                pathTag = pathTag.trim().substring("tag:".length(), pathTag.length());
                if (tags.contains(pathTag)) {
                    for (String fileTag : file) {
                        if (fileTag.equals(pathTag)) return true;
                    }
                }
            }
        }
        return false;
    }



    /**
	 * returns them in preferred order,
	 * host, file, tag, type, path, agent
	 * @return
	 */
	private DEF_FIELDS[] getDefaultValues() {
		return new DEF_FIELDS[] { DEF_FIELDS._host, DEF_FIELDS._filename, DEF_FIELDS._tag, DEF_FIELDS._type, DEF_FIELDS._path, DEF_FIELDS._agent, DEF_FIELDS._sourceUrl};
	}

//	public List<Field> getLiteralFields() {
//		List<Field> results = new ArrayList<Field>();
//		for (Field field : this.fields) {
//			if (field.isLiteralSynthetic()) results.add(field);
//		}
//		return results;
//	}
	
	public long lastModified() {
		return lastModified;
	}

    public boolean isStandardField(String fieldName) {
        return allFieldNamesCacheSet.contains(fieldName);
    }
	static public boolean isDefaultField(String fieldName) {
        if (!fieldName.startsWith("_")) return false;
		DEF_FIELDS[] values = FieldSet.DEF_FIELDS.values();
		for (DEF_FIELDS def_fields : values) {
			if (fieldName.equals(def_fields.name())) return true;
		}
		return false;
	}
    public boolean isDiscoveredField(String fieldName) {
        return dynamicFields.containsKey(fieldName);
    }

	public FieldSet copy() {

        try {
            while (true) {
                try {
                    synchronized (this.fields) {
                        byte[] serialize = Convertor.serialize(this);
                        return (FieldSet) Convertor.deserialize(serialize);
                    }
                } catch (ConcurrentModificationException e) {
                    Thread.sleep(50);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public List<FieldI> getFieldsByType(Class<?> type) {
        List<FieldI> results = new ArrayList<FieldI>();
        for (FieldI field : fields) {
            if (field.getClass().equals(type)) results.add(field);
        }
        return results;
    }

    public Number mapToView(String applyToGroup, Number value) {
        FieldI field = getField(applyToGroup);
        if (field != null) field.mapToView(value);
        return value;
    }

    public boolean upgrade() {
        if (this.fields.size() > 0) {
            if (this.fields.get(0).getClass().isAssignableFrom(Field.class)) {
                LOGGER.info("Upgrading:" + this.id);
                List<FieldI> newFields = new ArrayList<FieldI>();
                for (FieldI field : fields) {
                    Field src = (Field) field;
                    newFields.add(FieldFactory.getField(src.name(),src.synthExpression(), src.synthSource(), src.groupId(),
                            src.isVisible(), src.isSummary(), src.funct(), false));
                }
                this.fields = newFields;
                return true;
            }
        }
        return false;
    }
    public List<FieldDTO> toDTO() {
        List<FieldDTO> results = new ArrayList<FieldDTO>();
        ArrayList<FieldI> beanFields = new ArrayList<FieldI>();
        for (FieldI field : fields) {
            FieldDTO fieldI = field.toBasicField().toDTO();
            beanFields.add(fieldI);
            results.add(fieldI);
        }
        this.fields = beanFields;
        return results;
    }


    /**
     <name>client</name>
     <funct>count()</funct>
     <visible>true</visible>
     <summary>true</summary>
     <index>false</index>
     <description></description>
     <groupId>1</groupId>
     <synthSrcField>msg</synthSrcField>
     <synthExpression>substring,[client ,] </synthExpression>
     */
    public static class Field extends SynthFieldBase {
//        public String name;
//        public String funct;
//        public boolean visible;
//        public boolean summary;
//        public boolean index;
//        public String description;
//        public int groupId = -1;
//        private String literal;

        public Field() {
        }
        public Field(int groupId, String name, String function, boolean visible, boolean summary, String description, String synthSource, String synthExpr, boolean indexed) {
            super(name, groupId, visible, summary, function, indexed);
            setDescription(description);
            setSynthExpression(synthExpr);
            setSynthSource(synthSource);
        }

        @Override
        public String get(String[] events, Set<String> evalContext, FieldSet fieldSet) {
            return null;
        }

        public FieldDTO toDTO() {
            return new FieldDTO(name(), funct(), isVisible(),isSummary(),description(),groupId(), synthSource(), synthExpression(), isIndexed());
        }
    }
}
