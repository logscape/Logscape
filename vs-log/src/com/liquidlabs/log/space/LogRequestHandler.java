package com.liquidlabs.log.space;

import com.liquidlabs.transport.proxy.clientHandlers.Interceptor;
import com.liquidlabs.transport.proxy.Remotable;

import java.util.Map;

public interface LogRequestHandler extends Remotable {

	String getId();

    void cancel(LogRequest request);

    @Interceptor(clazz ="com.liquidlabs.log.search.DefaultSearchInterceptor")
	void replay(LogRequest request);
	@Interceptor(clazz ="com.liquidlabs.log.search.DefaultSearchInterceptor")
	void search(LogRequest request);


	Map<String,Double> volumes();
}
