package com.liquidlabs.transport.proxy;

import java.lang.reflect.Method;

/**
 */
public interface InvocationInterceptor {

    /**
     * Called on incoming impl (i.e. decode off the wire)
     */
    Object[] incoming(Object impl, Method method, Object[] args, InvocationInterceptor next);

    /**
     * Called on outgoing client side (i.e. encode before going on the wire)
     * @param impl
     * @param method
     * @param args
     * @param next
     * @return
     */
    Object[] outgoing(Object impl, Method method, Object[] args, InvocationInterceptor next);
}
