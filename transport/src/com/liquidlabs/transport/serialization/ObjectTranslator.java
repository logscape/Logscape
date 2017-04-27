package com.liquidlabs.transport.serialization;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportProperties;
import org.apache.log4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Convenience class that
 * - converts an Object into its String representation (comma sep string) <br>
 * - populates an Object from its String representation <br>
 * - allows the user to create a ObjectTranslator String to run against the space - <br>
 * i.e. create templateStrings using the field names of the object - <br>
 * => from "AField == 10 AND BField equals "STUFF" " to ",==:10,,,equals:STUFF," ".
 */
public class ObjectTranslator implements Translator {
    private static final String SCHEMA = "SCHEMA";

	private static final String QUOTES = "\"";

    private static final String QUOTE = "'";

    private final static Logger LOGGER = Logger.getLogger(ObjectTranslator.class);

    private static final String TEMPLATE_DELIM = ":";
    private static final String QUERY_ITEM_DELIM = " ";
    private static final String DELIMITER = Config.OBJECT_DELIM;
    private static final String EMPTY_STRING = "";
    private static final String OR = " OR ";

    private static final int defaultDepth = TransportProperties.getObjectGraphDepth();


    static Convertor convertor = new Convertor();
    static Matchers matchers = new Matchers();

    public String getStringFromObject(Object objectToFormat) {
        return getStringFromObject(objectToFormat, 1);
    }

    public String getStringFromObject(Object objectToFormat, int depth) {

        Class<?> type = objectToFormat.getClass();
        if (type.equals(String.class) || type.equals(Integer.class) || type.equals(Long.class) || type.equals(Double.class)) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(type.getName()).append(DELIMITER).append(objectToFormat);
            return stringBuilder.toString();
        }

        // skip proxyClient objects
        if (objectToFormat.getClass().toString().contains("class $Proxy")) {
            return String.format("%s%s", type.getName(), DELIMITER);
        }

        List<Field> fieldsList = getSortedFieldsList(type);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(type.getName());//.append(DELIMITER);
        boolean throwIt = false;
        for (Field field2 : fieldsList) {
            try {
                if (!Modifier.isTransient(field2.getModifiers())) {
                    field2.setAccessible(true);
                    Annotation[] annotations = field2.getAnnotations();

                    Object object = field2.get(objectToFormat);
                    if (annotations.length > 0 && annotations[0].toString().contains("Id") && object != null && object.toString().length() == 0) {
                        throwIt = true;
                        throw new RuntimeException("GIVEN Empty [] ID for:" + objectToFormat + " Field:" + field2.toString());
                    }
                    stringBuilder.append(DELIMITER).append(convertor.getStringFromObject(object, depth));

                }
            } catch (Throwable t) {
                LOGGER.warn("O:" + objectToFormat + "\n TYPE:" + type.getName() + "\n FIELD:" + field2.getName(), t);
                if (throwIt) throw new RuntimeException(t);
            }
        }
        return stringBuilder.toString();
    }

    private String getStringValueOfField(Object object, Field field) {
        boolean accesible = field.isAccessible();
        try {
            field.setAccessible(true);
            Object value = field.get(object);
            return convertor.getStringFromObject(value, 1);
        } catch (Throwable t) {
            LOGGER.warn(t);
        } finally {
            field.setAccessible(accesible);
        }
        return "";
    }

    /**
     * Updates an Object using query string syntax. Operators do nothing in this case.<br>
     * TODO: spaces not supported! <br>
     * i.e. query.getUpdatedObject(instance, "field1 = rubbish AND field2 = 1223");
     */
    public <T> T getUpdatedObject(T instance, String query) {
        if (query == null) query = "";
        List<Field> sortedFieldsList = getSortedFieldsList(instance.getClass());
        String[] splitQuery = Arrays.split(" ", query);
        for (Field field : sortedFieldsList) {
            if (query.contains(field.getName())) {
                String value = getValueFromString(splitQuery, field.getName());
                Object value2 = convertor.getObjectFromString(field.getType(), value, 3);
                try {
                    field.set(instance, value2);
                } catch (IllegalAccessException e) {
                    LOGGER.warn(e, e);
                }
            }

        }
        return instance;
    }

    private String getValueFromString(String[] query, String name) {
        int pos = 0;
        for (String string : query) {
            if (string.equalsIgnoreCase(name)) {
                return query[pos + 2];
            }
            pos++;
        }
        return null;
    }

    public boolean isMatch(String query, Object instance) {
        List<List<QueryData>> qd = getSomethingThatWorks(instance.getClass(), query);
        List<Field> fieldsList = getSortedFieldsList(instance.getClass());

        for (List<QueryData> queryList : qd) {
            int matches = 0;
            for (QueryData queryData : queryList) {
                String query2 = queryData.query;
            	String[] templates = new String[] { query2 };
				int matcherColumnCount = matchers.getMatcherColumnCount(templates);
            	for (int col = 0; col < matcherColumnCount; col++) {
            		matches += matchers.isMatch(new String[]{getStringValueOfField(instance, fieldsList.get(queryData.fieldPosition))}, templates, col) ? 1 : 0;                		
            	}
            }
            if (matches == queryList.size()) return true;
        }
        return false;

    }


    public String getAsCSV(Object objectToDisplay) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(objectToDisplay.getClass().getName()).append(",\n");
        List<Field> sortedFieldsList = getSortedFieldsList(objectToDisplay.getClass());
        for (Field field : sortedFieldsList) {
            try {
                stringBuilder.append(field.getName()).append("=").append(field.get(objectToDisplay)).append(",\n");
            } catch (Throwable t) {
                LOGGER.warn(t, t);
            }
        }
        return stringBuilder.toString();
    }

    public List<List<QueryData>> getSomethingThatWorks(Class<?> clazz, String query) {
        if (query == null) query = "";
        query = query.trim();
        // some querys may be submitted with 'someField equals "value"'
        if (query.startsWith(QUOTE) && query.endsWith(QUOTE)) query = query.substring(1, query.length() - 1);
        query = query.replaceAll(" and ", " AND ");
        query = query.replaceAll(" && ", " AND ");
        query = query.replaceAll(" or ", " OR ");
        query = query.replaceAll(" \\|\\| ", " OR ");
        String[] querySet = query.split(OR);
        List<Field> fieldsList = getSortedFieldsList(clazz);
        List<List<QueryData>> templates = new ArrayList<List<QueryData>>();
        for (String queryItem : querySet) {
            List<QueryData> qd = new ArrayList<QueryData>();
            String[] queryElement = queryItem.split("AND");
            for (String string : queryElement) {
                String[] split = getWhiteSpaceCorrectedSplit(string.trim().split(QUERY_ITEM_DELIM));
                if (split.length < 3) continue;
                String fieldName = split[0];
                String operator = split[1];
                String value = split[2];
                if (value.startsWith(QUOTES)) {
                    if (string.indexOf(QUOTES) == string.lastIndexOf(QUOTES))
                        throw new RuntimeException(String.format("Given bad String for:%s value:%s - expected opening and closing [\"]", clazz.getName(), value));
                    value = string.substring(string.indexOf(QUOTES) + 1, string.lastIndexOf(QUOTES));
                }
                if (value.startsWith("\'")) {
                    if (string.indexOf("\'") == string.lastIndexOf("\'"))
                        throw new RuntimeException(String.format("Given bad String for:%s value:%s - expected opening and closing [']", clazz.getName(), value));
                    value = string.substring(string.indexOf("\'") + 1, string.lastIndexOf("\'"));
                }
                int position = getFieldPosition(fieldsList, fieldName);
                qd.add(new QueryData(position, operator + TEMPLATE_DELIM + value));
            }
            templates.add(qd);
        }
        return templates;
    }

    private String[] getWhiteSpaceCorrectedSplit(String[] split) {
		List<String> results = new ArrayList<String>();
		for (String string : split) {
			if (string.length() > 0) results.add(string);
		}
		return Arrays.toStringArray(results);
	}

	class QueryData {
        public QueryData(int position, String string) {
            fieldPosition = position;
            query = string;
        }

        public int fieldPosition;
        public String query;
        @Override
        public String toString() {
        	return "Query, pos:" + fieldPosition + " q:" + query;
        }
    }

    public String[] getQueryStringTemplate(Class<?> clazz, String query) {

        try {
            if (query == null) query = "";
            query = query.trim();
            // some querys may be submitted with 'someField equals "value"'
            if (query.startsWith(QUOTE) && query.endsWith(QUOTE)) query = query.substring(1, query.length() - 1);
            query = escapeAND(QUOTE, query);
            query = escapeAND(QUOTES, query);
            query = query.replaceAll(" and ", " AND ");
            query = query.replaceAll(" or ", " OR ");

            String[] querySet = StringUtil.splitRE(OR, query);
            List<Field> fieldsList = getSortedFieldsList(clazz);
            String[] resultTemplates = new String[querySet.length];
            int templatePos = 0;

            for (String queryItem : querySet) {
                List<List<String>> resultTemplate = new ArrayList<List<String>>();
                for (@SuppressWarnings("unused") Field field : fieldsList) {
                    // "," place holder
                    ArrayList<String> arrayList = new ArrayList<String>();
                    resultTemplate.add(arrayList);
                }
                String[] queryElement = queryItem.split(" AND ");
                for (String string : queryElement) {
                    String[] split = string.trim().split(QUERY_ITEM_DELIM);
                    if (split.length >= 2) {
                        String fieldName = split[0];
                        String operator = split[1];
                        String value = string.substring(string.indexOf(operator) + operator.length()).trim();//split[2];
                        if (value.startsWith(QUOTE) && value.endsWith(QUOTE))
                            value = value.substring(1, value.length() - 1);
                        if (value.startsWith(QUOTES) && value.endsWith(QUOTES))
                            value = value.substring(1, value.length() - 1);

                        value = value.replaceAll(" A_N_D ", " AND ");

                        // should probably disable this....
                        if (operator.equalsIgnoreCase("equals") && value.length() == 0) {
                            throw new RuntimeException("Invalid Query Given: Clazz:" + clazz.toString() + " Empty Value given for:[" + string + "]");
                        }
                        int position = getFieldPosition(fieldsList, fieldName);
//					Class<?> type = fieldsList.get(position).getType();

//					if (value.startsWith("\"")) {
//						int indexOf = string.indexOf("\"");
//						int lastIndexOf = string.lastIndexOf("\"");
//						if (indexOf == lastIndexOf) value = string.substring(indexOf);
//						value = string.substring(indexOf+1, lastIndexOf);
//					}
//					if (value.startsWith("'")) {
//						int indexOf = string.indexOf("'");
//						int lastIndexOf = string.lastIndexOf("'");
//						if (indexOf == lastIndexOf) value = string.substring(indexOf);
//						else {
//							value = string.substring(indexOf+1, lastIndexOf);
//							
//							
//							// String types are encoded when in the space - so replaceWith and equals etc need to use the encoded format in order to match the stored value
////							if (type.equals(String.class)) {
////								value = convertor.encodeString(value);
////							}
//						}
//					}
                        // override place holder items
                        List<String> list = resultTemplate.get(position);
                        if (list.size() == 0) {
                            list.add(operator + TEMPLATE_DELIM + value);
                        } else {
                            // we have an 'AND' or 'OR' operation against an existing field
                            // for now just do AND
                            list.add("AND:" + operator + TEMPLATE_DELIM + value);
                        }
                    }
                }

                StringBuilder resultString = new StringBuilder();
                // Fill in object type as first item
                resultString.append("equals:").append(clazz.getName()).append(DELIMITER);
                for (List<String> resultValueList : resultTemplate) {
                    if (resultValueList.size() == 0) {
                        resultString.append(EMPTY_STRING).append(DELIMITER);
                    } else {
                        for (String resultValue : resultValueList) {
                            resultString.append(resultValue).append(DELIMITER);
                        }
                    }
                }
                resultTemplates[templatePos++] = resultString.toString();
            }
            return resultTemplates;
        } catch (Throwable t) {
            throw new RuntimeException(String.format("Could not process [%s] = query[%s]", clazz.getName(), query), t);
        }
    }

    String escapeAND(String splitter, String query) {
        StringBuilder result = new StringBuilder();
        String[] split = StringUtil.splitRE(splitter, query);
        boolean inQuotes = false;
        for (String string : split) {
            if (inQuotes) {
                result.append(splitter).append(string.replaceAll(" AND ", " A_N_D ")).append(splitter);
            } else result.append(string);
            inQuotes = !inQuotes;
        }
        return result.toString();
    }

    private int getFieldPosition(List<Field> fieldsList, String fieldName) {
        int pos = 0;
        for (Field field : fieldsList) {
            if (field.getName().equalsIgnoreCase(fieldName)) return pos;
            pos++;
        }
        throw new RuntimeException("Failed to find Field:" + fieldName + " from set:" + fieldsList);
    }

    /**
     * Populate a
     *
     * @param <T>
     * @param targetObject
     * @param valuesString
     * @return
     */
    public <T> T getObjectFromFormat(Class<T> type, String valuesString) {
        return getObjectFromFormat(type, valuesString, defaultDepth);
    }

    @SuppressWarnings("unchecked")
    public <T> T getObjectFromFormat(Class<T> type, String valuesString, int depth) {

        if (type.equals(void.class)) return null;

        String[] split = Arrays.split(DELIMITER, valuesString);
        if (type.equals(String.class)) return (T) split[1];
        if (type.equals(Integer.class)) return (T) Integer.valueOf(split[1]);
        if (type.equals(Long.class)) return (T) Long.valueOf(split[1]);
        if (type.equals(Double.class)) return (T) Double.valueOf(split[1]);
        if (type.equals(Boolean.class)) return (T) Boolean.valueOf(split[1]);
        if (type.isInterface()) {
            String contreteTypeClassName = split[0];
            try {
                type = (Class<T>) Class.forName(contreteTypeClassName);
            } catch (ClassNotFoundException e) {
                LOGGER.warn("ClassNotFound, ignoring:" + type.toString() + " v:" + valuesString, e);
                System.err.println("ObjectTranslator: ClassNotFound, ignoring:" + type.toString() + " v:" + valuesString + " ex:" + e.toString());
                return null;
            }
        }
        if (type.toString().contains("$Proxy")) {
            LOGGER.debug("Ignoring Proxy object instance:" + type.toString());
            return null;
        }
        T targetObject = getDefaultConstructedInstance(type);

        String[] values = split;
        boolean schemaBased = isSchemaBasedClass(targetObject.getClass());
        List<Field> sortedFieldsList = getSortedFieldsList(targetObject.getClass());
        // start from position 1
        if (!type.getName().equals(values[0])) {
            if (values[0].contains(type.getName())) {
                LOGGER.warn("Ignoring, unextracted  data (Check the file encoding is ISO-8859-1\n\t\tType:" + type.getName() + "\n\t\tValues[0]:" + values[0]);
                LOGGER.warn("Expected FieldCount;" + sortedFieldsList.size() + " SerializedFieldCount:" + values.length);
                System.err.println("ObjectTranslator: Ignoring, unextracted Type:" + type.getName() + " Values[0]:" + values[0]);
                return null;
            }
            throw new RuntimeException(String.format("Type mismatch between[%s] AND [%s] RAW[%s]", type.getName(), values[0], getLoggableString(valuesString)));

        }
        boolean warning = false;
        if (!schemaBased && values.length != sortedFieldsList.size() + 1) {
            int pos = 1;
            warning = true;
            LOGGER.warn(targetObject.getClass() + ": Size MisMatch - (StringLength):" + values.length + "<>" + (sortedFieldsList.size() + 1) + " (FieldLength)");
            LOGGER.warn("RAW[" + valuesString + "]");
            int fieldPos = 0;
            for (Field field2 : sortedFieldsList) {
                String value = pos + 1 > values.length ? "?" : values[pos];
                LOGGER.warn(fieldPos++ + " > Field:" + field2.getName() + "\t[" + value + "]");
                pos++;
            }
            LOGGER.warn("Object:" +  targetObject.getClass().toString() + " Fields: " + sortedFieldsList + " Values: " + Arrays.toString(values));
          //  throw new RuntimeException(String.format("Type:%s Expected [%d] fields, but got [%d] values[%s]", targetObject.getClass().toString(), (sortedFieldsList.size() + 1), values.length, getLoggableString(valuesString)));
        }
        int pos = 0;
        int valuePos = 0;
        Object firstValue = "";
        for (String value : values) {
            if (valuePos++ == 0) continue;
            Field field = null;
            try {

            	field = sortedFieldsList.get(pos++);
                if (warning) LOGGER.warn("Best Effort : Assignment :" + field + " =" + value);
                Object value2 = convertor.getObjectFromString(field.getType(), value, depth);
				field.set(targetObject, value2);
				if (valuePos == 1) firstValue = value2;
            } catch (Exception e) {
                String exMessage = e.getMessage();
                if (field != null && exMessage != null && !exMessage.contains("No enum const") && !exMessage.contains("Field is final") && !e.toString().contains("to java.lang.String")) {
                    LOGGER.warn(" Class:" + targetObject.getClass().getName() + firstValue + " fieldIndex:" + valuePos +  " ex:" + e.toString() + ", Ignoring field:" + field.getName(), e);
                }
            }
        }
        if (targetObject == null)
            throw new RuntimeException(String.format("Failed to decode type:%s from %s", type.getName().toString(), valuesString));
        return targetObject;
    }

    private boolean isSchemaBasedClass(Class<? extends Object> givenClass) {
        Class<?>[] declaredClasses = givenClass.getDeclaredClasses();
        for (Class<?> class1 : declaredClasses) {
            if (class1.getName().endsWith(SCHEMA)) {
                return true;
            }
        }
        return false;
    }

    private String getLoggableString(String valuesString) {
        return valuesString.length() > 256 ? valuesString.substring(0, 255) : valuesString;
    }

    @SuppressWarnings("unchecked")
    public <T> T getDefaultConstructedInstance(Class<T> type) {
        try {
            if (type.toString().contains("ArrayList")) {
                return (T) new ArrayList();
            }
            if (type.toString().contains("java.util.Collections$SynchronizedRandomAccessList")) {
                return (T) Collections.synchronizedList(new ArrayList());
            }
            Constructor<T> declaredConstructor = type.getDeclaredConstructor();
            return declaredConstructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to create DefaultInstance[%s]msg[%s]", type, e.getMessage()));
        }
    }

    private Map<Class<?>, List<Field>> sortedFieldsCache = new ConcurrentHashMap<Class<?>, List<Field>>();
    AtomicInteger hitCount = new AtomicInteger();

    public List<Field> getSortedFieldsList(Class<?> class1) {


        if (sortedFieldsCache.containsKey(class1)) {
            hitCount.getAndIncrement();
            if (LOGGER.isDebugEnabled()) LOGGER.debug("CacheHit:" + class1 + " hit Count:" + hitCount.get());
//			LOGGER.info("Hit:" + class1 + " hit Count:" + hitCount.get() + " cSize:" + sortedFieldsCache.size());

            return sortedFieldsCache.get(class1);
        }
        // extract the schema
        List<Field> fields = getSchemaBasedFields(class1);
        if (fields != null) {
            if (sortedFieldsCache.size() > 50) sortedFieldsCache.clear();
            sortedFieldsCache.put(class1, fields);
            return fields;
        }

        ArrayList<Field> fieldsList = getFieldFromClass(class1);
        Class<? extends Object> superClass1 = class1.getSuperclass();
        while (superClass1 != null) {
            fieldsList.addAll(getFieldFromClass(superClass1));
            superClass1 = superClass1.getSuperclass();
        }
        Collections.sort(fieldsList, new Comparator<Field>() {
            public int compare(Field o1, Field o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        if (sortedFieldsCache.size() > 50) sortedFieldsCache.clear();
        sortedFieldsCache.put(class1, fieldsList);
//		LOGGER.info("Miss:" + class1);
        return fieldsList;
    }

    public List<Field> getSchemaBasedFields(Class<?> givenClass) {


        Map<String, Field> fields = new HashMap<String, Field>();

        populateMap(getFieldFromClass(givenClass), fields);

        try {
            populateMap(getFieldFromClass(givenClass.getSuperclass()), fields);
        } catch (Throwable t){
        }

        try {
            populateMap(getFieldFromClass(givenClass.getSuperclass().getSuperclass()), fields);
        } catch (Throwable t){

        }


        Class<?>[] declaredClasses = givenClass.getDeclaredClasses();
        Class<?> enumClass = null;
        for (Class<?> class1 : declaredClasses) {
            if (class1.getName().endsWith(SCHEMA)) {
                enumClass = class1;
            }
        }
        if (enumClass == null) return null;
        Object[] e = (Object[]) enumClass.getEnumConstants();

        ArrayList<Field> results = new ArrayList<Field>();
        for (Object enumField : e) {
            String fieldName = enumField.toString().toUpperCase();
            try {
                Field e2 = fields.get(fieldName);
                if (e2 == null) {
                	LOGGER.warn("Failed to find FIELD:" + fieldName + " in CLASS:" + givenClass + " SCHEMA is VALID: Fields:" + fields.keySet() );
                } else {
                	results.add(e2);
                }
            } catch (Exception e1) {
                LOGGER.error(e1);
            }
        }
        return results;
    }

    private void populateMap(ArrayList<Field> fieldFromClass, Map<String, Field> fields) {
        for (Field field : fieldFromClass) {
            fields.put(field.getName().toUpperCase(), field);
        }
    }

    private ArrayList<Field> getFieldFromClass(Class<?> class1) {
        Field[] fields = class1.getDeclaredFields();
        AccessibleObject.setAccessible(fields, true);
        ArrayList<Field> fieldsList = new ArrayList<Field>();
        for (Field field : fields) {
            if (field.toString().contains("static")) continue;
            if (field.toString().contains("transient")) continue;
            if (field.toString().contains("enum")) continue;
            fieldsList.add(field);
        }
        return fieldsList;
	}
}

