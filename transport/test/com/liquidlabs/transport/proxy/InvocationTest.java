package com.liquidlabs.transport.proxy;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

import com.liquidlabs.transport.proxy.events.DefaultEventListener;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.serialization.Convertor;

public class InvocationTest {
	
	private String clientURI;
	private Convertor convertor;
	
	@Before
	public void setUp() throws Exception {
		clientURI = "tcp://localhost:800000/space/client";
		convertor = new Convertor();
	}
	@After
	public void tearDown() throws Exception {
	}
	
	public void testInvocationToStringPerformance() throws Exception {
//		new Invocation invocation = new Invocation(method, convertor);
//		byte[] bytes2 = invocation.toString().getBytes("UTF-8");
		
	}
	@Test
	public void testShouldPassReturnParamsAsNull() throws Exception {
		Method method = this.getClass().getMethod("methodWithObjectListParam", List.class, int.class);
		Invocation invocation = new Invocation(method, convertor);
		String invString = invocation.toString("guid", "destId", clientURI, "partitionName", new ArrayList<Integer>(), new Object[] { new ArrayList<SomeParam>(), 0} );
		
		Invocation invocation2 = new Invocation();
		invocation2.fromString(invString);
		String invWithNullParams = invocation2.toString(true);
		
		Invocation finalInv = new Invocation();
		finalInv.fromString(invWithNullParams);
	}
	@Test
	public void testShouldPassListArgumentProperly() throws Exception {
		Method method = this.getClass().getMethod("methodWithObjectListParam", List.class, int.class);
		Invocation invocation = new Invocation(method, convertor);
		String invString = invocation.toString("guid", "destId", clientURI, "partitionName", new ArrayList<Integer>(), new Object[] { new ArrayList<SomeParam>(), 0} );
		
		Invocation invocation2 = new Invocation();
		invocation2.fromString(invString);

		invocation2.getResult();
	}
	@Test
	@SuppressWarnings("unchecked")
	public void testShouldPassListResultProperly() throws Exception {
		Method method = this.getClass().getMethod("someCustomMethodWithListResult");
		Invocation invocation = new Invocation(method, convertor);
		String invString = invocation.toString("guid", "destId", clientURI, "partitionName", new ArrayList<Integer>(), new Object[0]);
		
		Invocation invocation2 = new Invocation();
		invocation2.fromString(invString);
		
		Object result = invocation2.getResult();
		assertTrue(result instanceof List);
		assertTrue( ((List) result).size() == 0);
	}
//	@Test   DodgyTest?
	public void XXXXfromStringIsDisabledAndHardCodedToCertainTypesXXtestShouldUseFromStringOnParametersEven() throws Exception {
		Method method = this.getClass().getMethod("methodWithFromStringParam", SomeParam.class);
		Invocation invocation = new Invocation(method, convertor);
		Object[] args = new Object[] { new SomeParam()  };
		String invString = invocation.toString("guid", "destId", clientURI, "partitionName", "result", args);
		Invocation invocation2 = new Invocation();
		invocation2.fromString(invString);
		Object arg = invocation2.getArg(0);
		assertEquals("this is valueFromString!",((SomeParam)arg).value);
	}
	@Test
	public void testShouldPassIntegerArrayProperly() throws Exception {
		Method method = this.getClass().getMethod("someCustomMethod2", Integer[].class);
		Invocation invocation = new Invocation(method, convertor);
		Object[] args = new Object[] { new Integer[] { 100,200,300 }  };
		String invString = invocation.toString("guid", "destId", clientURI, "partitionName", "result", args);
		
		Invocation invocation2 = new Invocation();
		invocation2.fromString(invString);

		Object[] args2 = invocation2.getArgs();
		Integer[] param1 = (Integer[]) args2[0];
		System.out.println(args2[0]);
		assertTrue(param1[0].equals(100));		
	}

	@Test
	public void testShouldPassThroughEventAsResult() throws Exception {
		Invocation invocation = new Invocation(getClass().getMethod("handleEvent"), convertor);
		Event event = new Event("id", "key", "value", Type.READ);
		String string = invocation.toString("guid", "destId", "udp", "partition", event);
		Invocation invocation2 = new Invocation();
		invocation2.fromString(string);

		assertNotNull(invocation2.getResult());
	}
	public Event handleEvent(){
		return null;
	}

	@Test
	public void testToAndStringNotifyMethod() throws Exception {
			Method method = getNotifyMethod();
			
			Invocation invocation = new Invocation(method, convertor);
			Object[] objArgs = new Object[5];
			objArgs[0] = new String[] { "key1", "key2"};
			objArgs[1] = new String[] { "templ1", "templ2"};
			objArgs[2] = new DefaultEventListener();
			objArgs[3] = new Type[] { Type.READ, Type.WRITE };
			objArgs[4] = -1;
			
			
			String spaceEventString = invocation.toString("guid", "destId", clientURI, "partitionName", "result", objArgs);
			Invocation invocation2 = new Invocation();
//			System.out.println(spaceEventString);
			invocation2.fromString(spaceEventString);
			assertEquals(clientURI, invocation.getClientURI());
			assertEquals(clientURI, invocation2.getClientURI());
			assertNotNull(invocation2.getResult());
	}
	
	@Test
	public void testShouldPassEmptyStringProperly() throws Exception {
		Method method = this.getClass().getMethod("stringArgMethod", String.class, String.class);
		Invocation invocation = new Invocation(method, convertor);
		Object[] args = new Object[] { "one", "" };
		String invString = invocation.toString("guid", "destId", clientURI, "partitionName", "result", args);
		
		Invocation invocation2 = new Invocation();
		invocation2.fromString(invString);
		Object[] args2 = invocation2.getArgs();
		assertEquals(2, args2.length);
		assertEquals("one", args2[0]);
		assertEquals("", args2[1]);
	}
	public void stringArgMethod(String s1, String s2){
	}
	@Test
	public void testShouldPassStringArrayProperly() throws Exception {
		Method method = this.getClass().getMethod("someCustomMethod", String.class, String.class, String.class);
		Invocation invocation = new Invocation(method, convertor);
		String testArg = "3,4,5,6,7,8,9";
		Object[] args = new Object[] { "one", "two", testArg };
		String invString = invocation.toString("guid", "destId", clientURI, "partitionName", "result", args);
		
		Invocation invocation2 = new Invocation();
		invocation2.fromString(invString);
		Object[] args2 = invocation2.getArgs();
		System.out.println(args2[2]);
		assertEquals(testArg, args[2]);
		
	}

	@Test
	public void testShouldGetRightMethodName() throws Exception {
		Method notifyMethod = getNotifyMethod();
		
//		String methodName = "public abstract java.lang.String com.liquidlabs.space.Space.notify(java.lang.String[],java.lang.String[],com.liquidlabs.space.notify.EventListener,com.liquidlabs.space.notify.Event$Type[],long)";
		Invocation invocation = new Invocation();
		invocation.setMethodName(notifyMethod.toString());
		Method method = invocation.getMethod();
		assertNotNull(method);
		assertTrue(method.getName().equals("notify"));
	}
	
	public String[] doStuff() {
		return null;
	}
	@Test
	public void testToFromStringArrayWithZeroArgs() throws Throwable {
		Method method = this.getClass().getMethod("doStuff");
		Invocation toInvocation = new Invocation(method, convertor);
		
		String[] resultStringArray = new String[] { "a0,a1", "b0,b2" };
		
		String serializedFormat = toInvocation.toString("guid", "destId", clientURI, "partitionName", resultStringArray);
	
		Invocation fromInvocation = new Invocation();
		System.err.println(">>>>" + serializedFormat);
		fromInvocation.fromString(serializedFormat);
		if (fromInvocation.isException()) throw fromInvocation.getException();
		Object[] args = fromInvocation.getArgs();
		assertEquals(0, args.length);
	}
	@Test
	public void testToAndStringWithZeroStringNotifyMethod() throws Exception {
			Method method = getNotifyMethod();
			
			Invocation spaceEvent = new Invocation(method, convertor);
			Object[] objArgs = new Object[5];
			objArgs[0] = new String[] { "key1", "key2"};
			objArgs[1] = new String[0];
			objArgs[2] = new DefaultEventListener();
			objArgs[3] = new Type[] { Type.READ, Type.WRITE };
			objArgs[4] = -1;
			
			
			String string = spaceEvent.toString("guid", "destId", clientURI, "partitionName", "result", objArgs);
			Invocation spaceEvent2 = new Invocation();
			spaceEvent2.fromString(string);
			assertEquals(2, ((String[]) spaceEvent2.getArg(0)).length);
			assertEquals(String[].class, spaceEvent2.getArg(1).getClass());
			assertEquals(Event.Type[].class, spaceEvent2.getArg(3).getClass());
		}
	@Test
	public void testToFromStringWithNullResult() throws Exception {
		Method method = getClass().getMethod("read", String.class);
		Invocation spaceEvent = new Invocation(method, convertor);
		String string = spaceEvent.toString("guid", "destId", clientURI, "partitionName", null, new Object[] { "arg1" } );
		Invocation spaceEvent2 = new Invocation();
		System.err.println( ">>>>" + string);
		spaceEvent2.fromString(string);
		assertNull("Result Should be null but was:" + spaceEvent2.getResult(), spaceEvent2.getResult());
	}
	@Test
	public void testToFromStringArrayWithResultIsPassed() throws Exception {
		Method method = getClass().getMethod("read", String[].class);
		Invocation spaceEvent = new Invocation(method, convertor);
		
		String[] result = new String[] { "a0,a1", "b0,b2" };
		
		
		String string = spaceEvent.toString("guid", "destId", clientURI, "partitionName", result, new Object[] { new String[] { "arg1", "arg2" } });
	
		Invocation spaceEvent2 = new Invocation();
		System.err.println(">>>>" + string);
		spaceEvent2.fromString(string);
		String[] fromString = (String[]) spaceEvent2.getResult();
		assertEquals(2, fromString.length);
	}
	@Test
	public void testShouldPassRawObjectThroughContentsAsArgument() throws Exception {
		Method method = getClass().getMethod("read", String.class);
		Invocation spaceEvent = new Invocation(method, convertor);
		
		
		String[] result = new String[] { "a0,a1", "b0,b2" };
		
		String string = spaceEvent.toString("guid", "destId", clientURI, "partitionName", result, "first,second,third,fourth,fifth");
	
		Invocation spaceEvent2 = new Invocation();
		System.err.println( ">>>>" + string);
		spaceEvent2.fromString(string);
		String args = (String) spaceEvent2.getArg(0);
		assertNotNull(args);
		assertEquals(1, spaceEvent2.getArgs().length);
	}
	
	@Test
	public void testFromNotifyMethod() throws Exception {
		Method method = getNotifyMethod();
//		public void notify(String[] keys, String[] templates, EventListener listener, Type[] eventMask, long expires);

		Invocation spaceEvent = new Invocation(method, convertor);
		Object[] objArgs = new Object[5];
		objArgs[0] = new String[] { "key1", "key2"};
		objArgs[1] = new String[] { "templ1", "templ2"};
		objArgs[2] = new DefaultEventListener();
		objArgs[3] = new Type[] { Type.READ };
		objArgs[4] = -1;
		
		
		String string = spaceEvent.toString("guid", "destId", clientURI, "partitionName", "result", objArgs);
		System.err.println(">>>>>>>" + string);
		assertEquals(clientURI, spaceEvent.getClientURI());
	}
	@Test
	public void testFromMethod() throws Exception {
		Method method = getClass().getMethod("read", String.class);
		Invocation spaceEvent = new Invocation(method, convertor);
		String string = spaceEvent.toString("guid", "destId", clientURI, "partitionName", "result", new Object[] { "arg1" } );
		System.err.println(">>>>" + string);
		assertEquals(clientURI, spaceEvent.getClientURI());
	}
	
	
	private Method getNotifyMethod() {
		Method notifyMethod = null;
		Method[] methods = getClass().getDeclaredMethods();
		for (Method method : methods) {
			if (method.getName().equals("notify")){
				notifyMethod = method;
			}
		}
		return notifyMethod;
	}
	
	public String someCustomMethod(String firstArg, String secondArg, String thirdArg){
		return "stuff";
	}
	
	public String someCustomMethod2(Integer[] values){
		return "stuff";
	}
	public List<Integer> someCustomMethodWithListResult(){
		return new ArrayList<Integer>();
	}
	
	public void methodWithFromStringParam(SomeParam someParam){
		
	}
	public void methodWithObjectListParam(List<SomeParam> someParam, int doStuff){
	}
	public String notify(String[] keys, String[] templates, EventListener listener, Type[] eventMask, long expires) {
		return "";
	}
	public String[] read(String[] key) {
		return null;
	}
	public String[] read(String key) {
		return new String[0];
	}

	public static class SomeParam {
		public SomeParam(){};
		String value = "defaultValue";
		public void fromString(String string){
			assertEquals("valueToString", string);
			this.value = "this is valueFromString!";
		}
		public String toString(){
			return "valueToString";
		}
	}
}
