package com.logscape.disco.indexer.mapdb;

import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.logscape.disco.indexer.mapdb.DbWrapper;
import com.logscape.disco.indexer.mapdb.MapDb;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class MapDbTest {

    private MapDb<Integer, String> mapDb;
    private MapDb dbTwo;
    private DB db;
    private DB db2;

    @Before
    public void setUp() throws Exception {
        db = makeDb(new File("build/" + "MapDbTestOne"));
        mapDb = new MapDb(db, "t");
        db2 = makeDb(new File("build/" + "MapDbTest2"));
        dbTwo = new MapDb(db2, "t2");
    }

    @After
    public void after() throws Exception {
        db.close();
        db2.close();
    }

    private DB makeDb(File file) {
        file.delete();
        new File("build").mkdirs();
        DBMaker dbMaker = DBMaker.newFileDB(file);
        return dbMaker.transactionDisable().make();
    }

    @Test
    public void shouldAddGetRemove() {

        mapDb.put(1, "hi");
        mapDb.put(2, "bye");
        assertThat(mapDb.get(1), is("hi"));
        assertThat(mapDb.get(2), is("bye"));

        mapDb.remove(1, 2);
        assertThat(mapDb.get(1), is(nullValue()));
        assertThat(mapDb.get(2), is("bye"));

        mapDb.remove(2, 3);
        assertThat(mapDb.get(2), is(nullValue()));

    }

    @Test
    public void shouldSeeTryOutCompact() {
        final DbWrapper<Integer, String> wrapper = new DbWrapper(mapDb, dbTwo, "name", new LoggingEventMonitor());
        wrapper.put(1, "string one");
        wrapper.put(2, "string two");
        wrapper.put(3, "string three");
        wrapper.remove(3, 4);
        wrapper.compact();
        assertThat(wrapper.get(1), is("string one"));
        assertThat(wrapper.get(4), is(nullValue()));
    }
}
