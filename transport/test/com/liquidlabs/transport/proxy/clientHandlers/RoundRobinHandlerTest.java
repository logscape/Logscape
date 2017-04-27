package com.liquidlabs.transport.proxy.clientHandlers;

import java.lang.reflect.Method;
import com.liquidlabs.common.net.URI;
import java.util.List;

import com.liquidlabs.transport.proxy.ProxyCaller;
import com.liquidlabs.transport.proxy.RetryInvocationException;
import com.liquidlabs.transport.proxy.clientHandlers.RoundRobin;
import com.liquidlabs.transport.proxy.clientHandlers.RoundRobinHandler;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;
import org.jmock.core.Constraint;

import com.liquidlabs.transport.proxy.addressing.AddressHandler;
import com.liquidlabs.transport.proxy.addressing.KeepOrderedAddresser;

public class RoundRobinHandlerTest extends MockObjectTestCase {
	
	private RoundRobinHandler handler;

	@Override
	protected void setUp() throws Exception {
		handler = new RoundRobinHandler();
	}
	
	public static class TestClass {
		
		@RoundRobin(factor=1)
		public void rotateOne(){
		}
		@RoundRobin(factor=2)
		public void rotateTwo(){
		}
	}
	
	
	public void testShouldGetNextAddressPastTheEnd() throws Exception {
		AddressHandler addressHandler = new KeepOrderedAddresser();
		addressHandler.addEndPoints("tcp://aOne:0", "tcp://bTwo:0");
		List<URI> endPointURIs = addressHandler.getEndPointURIs();
		
		URI nextURI = handler.getNextURI(endPointURIs.get(1), endPointURIs);
		assertEquals("tcp://aOne:0", nextURI.toString());
		
	}
	
	public void testShouldRotateFromEndToStart() throws Exception {
			
		AddressHandler addressHandler = new KeepOrderedAddresser();
		addressHandler.addEndPoints("tcp://aOne:0", "tcp://bTwo:0");
		Mock proxyCaller = mock(ProxyCaller.class);
		
		Method method = TestClass.class.getMethod("rotateOne");
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] { eq(method), eq(new URI("tcp://bTwo:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] { eq(method), eq(new URI("tcp://aOne:0")), ANYTHING, eq(false), ANYTHING }).will(returnValue(null));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] { eq(method), eq(new URI("tcp://bTwo:0")), ANYTHING, eq(false), ANYTHING }).will(returnValue(null));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] { eq(method), eq(new URI("tcp://aOne:0")), ANYTHING, eq(false), ANYTHING }).will(returnValue(null));
//		proxyCaller.clientExecute(method, methodState.currentURI, args, false);
		
		handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
        handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
        handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
        handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
	}
	
	public void testShouldNotReUseBlackList() throws Exception {
		
		AddressHandler addressHandler = new KeepOrderedAddresser();
		addressHandler.addEndPoints("tcp://aOne:0", "tcp://aTwo:0");
		Mock proxyCaller = mock(ProxyCaller.class);
		
		Method method = TestClass.class.getMethod("rotateOne");
		
		// 2 assertions for retry behaviour
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] { eq(method), eq(new URI("tcp://aTwo:0")), ANYTHING, eq(false), ANYTHING }).will(returnValue(null));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] { eq(method), eq(new URI("tcp://aOne:0")), ANYTHING, eq(false), ANYTHING }).will(throwException(new RetryInvocationException("booom")));
		
		handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
		
	}

	public void testShouldRotateWithOneAddressAndFailure() throws Exception {
		
		AddressHandler addressHandler = new KeepOrderedAddresser();
		addressHandler.addEndPoints("tcp://aOne:0", "tcp://aTwo:0");
		Mock proxyCaller = mock(ProxyCaller.class);
		
		Method method = TestClass.class.getMethod("rotateOne");
		
		// 2 assertions for retry behaviour
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] { eq(method), eq(new URI("tcp://aTwo:0")), ANYTHING, eq(false), ANYTHING }).will(returnValue(null));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] { eq(method), eq(new URI("tcp://aOne:0")), ANYTHING, eq(false), ANYTHING }).will(throwException(new RetryInvocationException("booom")));
		
		handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
		
	}
	public void testShouldRotateWithOneAddress() throws Exception {
		
		AddressHandler addressHandler = new KeepOrderedAddresser();
		addressHandler.addEndPoints("tcp://aOne:0");
		Mock proxyCaller = mock(ProxyCaller.class);
		
		Method method = TestClass.class.getMethod("rotateOne");
		
		// 2 assertions for retry
//		proxyCaller.expects(once()).method("clientExecute").with(eq(method), eq(new URI("tcp://aOne")), ANYTHING).will(throwException(new RetryInvocationException("booom")));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://aOne:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://aOne:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
		
		handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
		handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
		
	}
	public void testShouldRotateWithRetryException() throws Exception {
		
		AddressHandler addressHandler = new KeepOrderedAddresser();
		addressHandler.addEndPoints("tcp://aOne:0", "tcp://bTwo:0", "tcp://cThree:0");
		Mock proxyCaller = mock(ProxyCaller.class);
		
		Method method = TestClass.class.getMethod("rotateOne");
		
		// 2 assertions for retry
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://aOne:0")), ANYTHING, eq(false), ANYTHING } ).will(throwException(new RetryInvocationException("booom")));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://bTwo:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
		
		handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
		
	}
	
	public void testShouldRotateWithFactor1() throws Exception {
		
		AddressHandler addressHandler = new KeepOrderedAddresser();
		addressHandler.addEndPoints("tcp://aOne:0", "tcp://bTwo:0", "tcp://cThree:0");
		Mock proxyCaller = mock(ProxyCaller.class);
		
		Method method = TestClass.class.getMethod("rotateOne");
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://aOne:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://bTwo:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://cThree:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
		handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
		handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
		handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
	}
	public void testShouldRotateWithFactor2() throws Exception {
		
		AddressHandler addressHandler = new KeepOrderedAddresser();
		addressHandler.addEndPoints("tcp://aOne:0", "tcp://bTwo:0", "tcp://cThree:0");
		Mock proxyCaller = mock(ProxyCaller.class);
		
		Method method = TestClass.class.getMethod("rotateTwo");
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://aOne:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://aOne:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://bTwo:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
        handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
        handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
        handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
		
	}
	public void testShouldRotateWithFactor22() throws Exception {
		
		AddressHandler addressHandler = new KeepOrderedAddresser();
		addressHandler.addEndPoints("tcp://aOne:0", "tcp://bTwo:0", "tcp://cThree:0");
		Mock proxyCaller = mock(ProxyCaller.class);
		
		Method method = TestClass.class.getMethod("rotateTwo");
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://aOne:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://aOne:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://bTwo:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
		proxyCaller.expects(once()).method("clientExecute").with(new Constraint[] {eq(method), eq(new URI("tcp://bTwo:0")), ANYTHING, eq(false), ANYTHING } ).will(returnValue(null));
        handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
        handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
        handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
        handler.invoke((ProxyCaller) proxyCaller.proxy(), addressHandler, method, new Object[0]);
	}

}
