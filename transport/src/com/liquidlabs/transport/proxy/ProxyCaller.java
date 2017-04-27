package com.liquidlabs.transport.proxy;

import java.io.IOException;
import java.lang.reflect.Method;
import com.liquidlabs.common.net.URI;

public interface ProxyCaller {

	Object clientExecute(Method method, URI endPoint, Object[] args, boolean verbose, int ttlOverride) throws IOException, RetryInvocationException, InterruptedException;

    Object sendWithRetry(Method method, Object[] args)  throws IOException, InterruptedException;


}
