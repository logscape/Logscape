package com.liquidlabs.log.search.functions;

import java.util.Map;

/**
 * Created by neil.avery on 21/12/2015.
 */
public interface GlobalFunction extends Function {

    void handle(Map<String, Object> values, ValueSetter vs);
}
