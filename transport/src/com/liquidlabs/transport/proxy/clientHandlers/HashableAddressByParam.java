package com.liquidlabs.transport.proxy.clientHandlers;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Makes sticky endpoints based upon the method parm (default 1)
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)

public @interface HashableAddressByParam {
	int param() default 1;
}
