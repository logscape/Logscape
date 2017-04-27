package com.liquidlabs.transport.proxy.clientHandlers;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;



/**
 * If there is a problem sending the msg it will catch and log the exception without retrying,
 * It will also throw an InterruptedException
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface FailFastOnce {
	int ttl() default 60;
}
