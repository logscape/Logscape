package com.liquidlabs.transport.proxy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Note: Replays can only work on one way calls. Any leasing
 * info should be pass into the call.
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ReplayOnAddressChange {}
