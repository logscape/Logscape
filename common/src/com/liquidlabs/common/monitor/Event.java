package com.liquidlabs.common.monitor;

import java.util.HashMap;
import java.util.Map;

public class Event {
    private final String name;
    private final Map<String, Object> stuff = new HashMap<String, Object>();

    public Event(String name) {
        this.name = name;
    }

    private Event(String name, Map<String, Object> stuff, String key, Object value) {
        this.name = name;
        this.stuff.putAll(stuff);
        this.stuff.put(key, value);
    }

    public Event took(long duration) {
        return new Event(name, stuff, "took", duration);
    }

    public Event with(String key, Object value) {
        return new Event(name, stuff, key, value);
    }

    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder(" event:").append(name);
        for (Map.Entry<String, Object> entry : stuff.entrySet()) {
            result.append(" ").append(entry.getKey()).append(":").append(entry.getValue());
        }
        return result.toString();
    }
}
