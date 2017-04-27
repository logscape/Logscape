package com.liquidlabs.transport.proxy;

import com.liquidlabs.transport.serialization.Convertor;

public interface Partial {
	    InvocationState parseStringToObject(String line, Invocation spaceEvent, Convertor convertor);
	    void parseObjectToString(Invocation spaceEvent, StringBuilder stringBuilder, Convertor convertor, boolean nulifyParams);
}
