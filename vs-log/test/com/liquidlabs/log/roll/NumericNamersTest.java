package com.liquidlabs.log.roll;

import junit.framework.Assert;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 16/02/15
 * Time: 19:56
 * To change this template use File | Settings | File Templates.
 */
public class NumericNamersTest {

    @Test
    public void shouldDetectFormatWithDotsOnPath() {
        String rollTo= "/home/logscape/Logscape-2.5.1_b0224/logscape/work/LogServer_SERVER_/lab-failover/home/logscape/tickets/ticket252_scripted/test.log.1.gz";
        NumericNamers.NumericNamer namers1 = NumericNamers.getNumericNamers(rollTo);
        String s = namers1.get(rollTo, 10);
        Assert.assertEquals("/home/logscape/Logscape-2.5.1_b0224/logscape/work/LogServer_SERVER_/lab-failover/home/logscape/tickets/ticket252_scripted/test.log.10.gz",  s);

    }


    @Test
    public void shouldDetectFormat() {
        NumericNamers.NumericNamer namers1 = NumericNamers.getNumericNamers("test.log.1.gz");
        Assert.assertNotNull(namers1);
        Assert.assertEquals("test.log.2.gz", namers1.get("test.log.1.gz",2));

        NumericNamers.NumericNamer namers2 = NumericNamers.getNumericNamers("a/b/c.d/test.log.1");
        Assert.assertNotNull(namers2);
        Assert.assertEquals("test.log.2", namers2.get("test.log.1",2));
    }
}
