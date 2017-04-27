package com.liquidlabs.transport.serialization;

public interface Translator {

	String getStringFromObject(Object objectToFormat);

	String getStringFromObject(Object objectToFormat, int depth);

	<T> T getUpdatedObject(T instance, String query);

	boolean isMatch(String query, Object instance);

	String getAsCSV(Object objectToDisplay);

	String[] getQueryStringTemplate(Class<?> clazz, String query);

	<T> T getObjectFromFormat(Class<T> type, String valuesString);

	<T> T getObjectFromFormat(Class<T> type, String valuesString, int depth);

}
