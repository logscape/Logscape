package com.logscape.disco.indexer.persistit;

import com.persistit.*;
import org.junit.Test;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 21/11/2013
 * Time: 17:03
 * To change this template use File | Settings | File Templates.
 */
public class PersistItSpikeTest {


    @Test
    public void shouldDoNothing() {

    }

    public static void main(String[] args) throws Exception {
        // This program uses PersistitMap, an implementation of
        // java.util.SortedMap to store a copy of the system properties.
        // Each time you run this program it will compare the current
        // system properties to those that were stored previously and will
        // display any differences.
        //
        // PersistitMap works just like any other Map except that its values
        // are persistent. (See the API documentation for PersistitMap for
        // a few other considerations.)
        //
        new File("vs-log/build/persistit-spike").mkdirs();
        System.out.println("WorkingDir:" + new java.io.File(".").getAbsolutePath());
        Properties prop = new Properties();
        //prop.load(new FileInputStream("./vs-log/persistit.properties"));
        prop.put("datapath","vs-log/build/persistit-spike");
        prop.put("buffer.count.8192","32");
        prop.put("logfile","${datapath}/pmdemo.log");
        prop.put("volume.1","${datapath}/pmdemo,create,pageSize:8192,initialPages:5,extensionPages:5,maximumPages:100000");
        prop.put("journalpath","${datapath}/pmdemo");
        System.out.println("GOT:" + prop);

        Persistit persistit = new Persistit(prop);

        try {
            Exchange dbex = persistit.getExchange("pmdemo", "properties", true);
            //
            // Create a PersistitMap over this exchange. The map will be
            // non-empty if this program has already been run previously.
            //
            PersistitMap<Object, Object> persistitMap = new PersistitMap<Object, Object>(dbex);
            //
            // All Persistit database operations below are invoked through the
            // Map interface.
            //
            if (persistitMap.size() == 0) {
                System.out.println("This is the first time PersistitMapDemo has run");
            } else {
                System.out.println("Comparing property values:");
                SortedMap<Object, Object> sorted = new TreeMap<Object, Object>(System.getProperties());

                for (Iterator<Map.Entry<Object, Object>> iter = sorted.entrySet().iterator(); iter.hasNext();) {
                    Map.Entry<Object, Object> entry = iter.next();
                    Object name = entry.getKey();
                    Object newValue = entry.getValue();
                    Object oldValue = persistitMap.remove(name);
                    if (oldValue == null) {
                        System.out.println("New value " + name + " is '" + newValue + "'");
                    } else if (!newValue.equals(oldValue)) {
                        System.out.println("Value changed " + name + " from '" + oldValue + "' to '" + newValue
                                + "'");
                    }
                }
            }

            for (Iterator<Map.Entry<Object, Object>> iter = persistitMap.entrySet().iterator(); iter.hasNext();) {
                Map.Entry<Object, Object> entry = iter.next();
                Object name = entry.getKey();
                Object oldValue = entry.getValue();
                System.out.println("Old value " + name + "='" + oldValue + "' is gone");
                iter.remove();
            }

            persistitMap.putAll(System.getProperties());

        } finally {
            // Always close Persistit. If the application does not do
            // this, Persistit's background threads will keep the JVM from
            // terminating.
            //
            persistit.close();
        }
    }
}
