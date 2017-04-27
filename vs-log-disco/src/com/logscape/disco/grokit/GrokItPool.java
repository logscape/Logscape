package com.logscape.disco.grokit;

import com.liquidlabs.common.Pool;

import java.lang.management.ManagementFactory;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 23/06/2014
 * Time: 13:04
 * To change this template use File | Settings | File Templates.
 */
public class GrokItPool implements Pool.Factory {
    private Pool pool;

    @Override
    public Object newInstance() {
        return new GrokIt();
    }
    public void process(String line) {

    }

    public GrokItPool() {
    }

    private Pool getPool(int cores) {
        return new Pool(cores, cores, this);
    }

    public Map<String, String> processLine(String filename, String data) {
        if (this.pool == null) {
            int cores = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
            this.pool = getPool(cores);
        }

        GrokIt grokIt = (GrokIt) this.pool.fetchFromPool();
        Map<String, String> results = null;
        try {
            results = grokIt.processLine(filename, data);
        } finally {
            this.pool.putInPool(grokIt);
        }
        return results;
    }
}
