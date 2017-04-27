package com.liquidlabs.transport.proxy.clientHandlers;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;



/**
 * In event of error it kills the proxyClient by calling stop() and prevents any further msgs being sent. 
 * Once disabled, all subsequent calls will fall through and return null.
 * 
 * Use with bursty traffic where client is unreliable and you dont care about msg delivery.
 * 
 * Ideally the client will keep registering the proxy client and when that happens the cached version will
 * be retrieved from the ProxyFactory and re-started
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface FailFastAndDisable {}
