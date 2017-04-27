package com.liquidlabs.transport.proxy.clientHandlers;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Caches invocation results for a TTL time (default 60 seconds)
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)

public @interface Cacheable {
	int ttl() default 60;
}
