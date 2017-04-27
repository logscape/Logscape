package com.liquidlabs.vso.work;

/**
 * 
 * Any service that wished to integrate with the dashboard must implement this method
 *
 */
public interface Invokable {
	
	/**
	 * 
	 * @return XML describing UI layout
	 "<panel>" +
		"	<title label='Last Panel'/>"+
		"	<row spaceOneWith='20' " +
					"label='Last it will work' labelWith='200' " +
					"buttonLabel='push me' buttonWidth='200' target='output' arg='hardParam' " +
					"method='doStuff' " + 
					"inputLabel='param' inputWidth='0' " +
					"outputLabel='out' outputWidth='200' olVisible='true'   " +
					"taText='' taWidth='0' taHeight='0'  taVisible='false'   />"+
		"	<row spaceOneWith='20' " +
					"label='LAST will it work - it better' labelWith='200' " +
					"buttonLabel='push me' buttonWidth='200' target='textArea' argWidget='input' " +
					"method='doStuff' " + 
					"inputLabel='param' inputWidth='200' " +
					"outputLabel='out' outputWidth='0'    " +
					"taText='' taWidth='400' taHeight='20'    />"+
		"</panel>" +
	 */
	String getUI();
	
	String invoke(String methodName, String arg);
	
	/**
	 * Any method with the signature "String methodName(String anySingleArg)" is supported for invocation
	 * and to be invoked need to be embedded in the UI.XML
	 * 
	 */

}
