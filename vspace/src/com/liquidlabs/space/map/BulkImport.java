package com.liquidlabs.space.map;

import java.util.Date;

import org.prevayler.Transaction;


public class BulkImport implements Transaction {
    private java.util.Map<String, String> data;
    private boolean merge;
	private boolean overwrite;

    public BulkImport(java.util.Map<String, String> data, boolean merge, boolean overwrite) {
        this.data = data;
        this.merge = merge;
        this.overwrite = overwrite;
    }

    public void executeOn(Object o, Date date) {
        Map mapImpl = (Map) o;
        try {
            mapImpl.importData(data, merge, overwrite);
        } catch (MapIsFullException e) {
        }
    }
}
