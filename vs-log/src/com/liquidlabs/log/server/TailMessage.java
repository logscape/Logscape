package com.liquidlabs.log.server;

import java.util.ArrayList;
import java.util.List;

public class TailMessage {
    private final String uuid = "tail";
    private final String event;
    public long fromLineNumber;
    private final List<String> data = new ArrayList<String>();

    public TailMessage(String event) {
        this.event = event;
    }

    public void add(String line) {
        data.add(line);
        if (data.size()  > 100) data.remove(0);
    }

    public boolean hasData() {
        return !data.isEmpty();
    }

}
