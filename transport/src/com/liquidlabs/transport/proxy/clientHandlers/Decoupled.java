package com.liquidlabs.transport.proxy.clientHandlers;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * As the names say - decouple the invocation to allow the calling thread to be released.
 * Used when called wants invocation retry and to release calling thread for other purposes.
 * Can be used to prevent invocation deadlock  when network invocation re-entrancy occurs 
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)

public @interface Decoupled {
}
