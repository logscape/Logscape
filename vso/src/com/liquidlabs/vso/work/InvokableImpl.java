package com.liquidlabs.vso.work;

import java.lang.reflect.Method;

import org.apache.log4j.Logger;


/**
 * Remote object proxy bridge that supports simple 'method, arg' invocation
 * Its used to overcome classloading issue by allowing remote invocation where the destination
 * class is not loaded. i.e. from vscape runtime into a deployed bundles service
 * 
 * Sample UI looks like this... (from DashboardServiceStub) - see bottom of file for larger example
 * 
 * 	"<root><panel>" +
		"	<title label='Last Panel'/>"+
		"	<label label='this is a label 1' padding='10'/>"+
		"	<label label='this is a label 2' padding='20'/>"+
		"	<label label='this is a label 3' padding='30'/>"+
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
		"</panel></root>" +
 *
 */
public class InvokableImpl implements Invokable {

	private static final Logger LOGGER = Logger.getLogger(InvokableImpl.class);
	
	private final InvokableUI delegate;

	public InvokableImpl(InvokableUI delegate){
		this.delegate = delegate;
		
	}

	public String getUI() {
		return delegate.getUI();
	}

	public String invoke(String methodName, String arg) {
		try {
			Method method2 = delegate.getClass().getMethod(methodName, String.class);
			return (String) method2.invoke(delegate, arg);
		} catch (Throwable e) {
			LOGGER.error(e.getMessage(), e);
			return e.getMessage();
		}
	}
	
	/**
	 * 	"<root>"+
		"<panel>" +
		"	<title label='First Panel:" + new Date().toString() + "'/>"+
		"	<row spaceOneWith='20' " +
				"label='FIRST will it work' labelWith='200' " +
					"buttonLabel='push me' buttonWidth='200' target='output' argWidget='input' " +
					"method='doStuff' " + 
					"inputLabel='param' inputWidth='200' " +
					"outputLabel='out' outputWidth='200' olVisible='true'   " +
					"taText='' taWidth='0' taHeight='0' taVisible='false'  />"+
		"	<row spaceOneWith='20' " +
					"label='FIRST will it work now?' labelWith='200' " +
					"buttonLabel='push me' buttonWidth='200' target='textArea' argWidget='input' " +
					"method='doStuff' " + 
					"inputLabel='param' inputWidth='200' " +
					"outputLabel='out' outputWidth='0'    " +
					"taText='' taWidth='400' taHeight='20'     />"+
		"</panel>" +
		"<panel/>" +
		"<panel>" +
		"	<title label='Last Panel'/>"+
		"	<label label='this is a label 1' padding='10'/>"+
		"	<label label='this is a label 2' padding='20'/>"+
		"	<label label='this is a label 3' padding='30'/>"+
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
		"</root>";
	 */

}
