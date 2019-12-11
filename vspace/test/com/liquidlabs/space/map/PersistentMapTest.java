/**
 */
package com.liquidlabs.space.map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;
import org.prevayler.Clock;
import org.prevayler.Prevayler;
import org.prevayler.Query;
import org.prevayler.SureTransactionWithQuery;
import org.prevayler.Transaction;
import org.prevayler.TransactionWithQuery;

import java.io.File;
import java.io.IOException;
import java.util.Date;

public class PersistentMapTest {

    class PrevaylerStub implements Prevayler {
        private Map map;

        PrevaylerStub(Map map) {
            this.map = map;
        }

        public Object prevalentSystem() {
            return map;
        }

        public Clock clock() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public void execute(Transaction transaction) {
            transaction.executeOn(map, new Date());
        }

        public Object execute(Query query) throws Exception {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public Object execute(TransactionWithQuery transactionWithQuery) throws Exception {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public Object execute(SureTransactionWithQuery sureTransactionWithQuery) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public File takeSnapshot() throws IOException {
            return null;
        }

        public void close() throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }

	private ArrayStateSyncer stateSyncer;

    @Test
    public void shouldExportMap() throws MapIsFullException {

        Map map = new MapImpl("foo", "bar", 6700, false, stateSyncer);
        PersistentMap pm = new PersistentMap(new PrevaylerStub(map));
        pm.put("value", "data");
        pm.put("v1", "d1");
        pm.put("v2", "d2");

        java.util.Map map1 = pm.export();
        assertEquals("data", map1.get("value"));
        assertEquals("d1", map1.get("v1"));
        assertEquals("d2", map1.get("v2"));
    }

    @Test
    public void shouldExportACopyOfMap() throws MapIsFullException {
        Map map = new MapImpl("foo", "bar", 6700, false, stateSyncer);
        PersistentMap pm = new PersistentMap(new PrevaylerStub(map));
        pm.put("value", "data");

        java.util.Map map1 = pm.export();
        map1.put("foo", "moo");
        assertNull(pm.get("foo"));
    }
}
