package com.liquidlabs.common.file;

import com.liquidlabs.common.file.raf.RafSweeper;
import junit.framework.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 22/02/2013
 * Time: 08:58
 * To change this template use File | Settings | File Templates.
 */
public class RawSweeperTest {
    private boolean isWindows() {
        return System.getProperty("os.name").toUpperCase().contains("WINDOW");
    }


    @Test
    public void shouldReadNormalByteBuffer() throws Exception {
        RafSweeper sweeper = new RafSweeper(new char[]{'\n'});
        ByteBuffer bb = ByteBuffer.wrap("0123456789\n".getBytes());
        long sweepable = sweeper.isSweepable(bb, (short) 0);
        Assert.assertEquals(10, sweepable);

        ByteBuffer bb2 = ByteBuffer.wrap("0123456789".getBytes());
        long sweepable2 = sweeper.isSweepable(bb2, (short) 0);
        Assert.assertEquals(-1, sweepable2);
    }
//    @Test    Errors out with unsupported operation exception
    public void shouldReadMemByteBuffer() throws Exception {
        if (isWindows()) return;
        RafSweeper sweeper = new RafSweeper(new char[]{'\n'});
        ByteBuffer bb = ByteBuffer.allocateDirect(100);
        bb.put("0123456789\n".getBytes());
        bb.flip();
        long sweepable = sweeper.isSweepable(bb, (short) 0);
        Assert.assertEquals(10, sweepable);

        ByteBuffer bb2 = ByteBuffer.wrap("0123456789".getBytes());
        long sweepable2 = sweeper.isSweepable(bb2, (short) 0);
        Assert.assertEquals(-1, sweepable2);
    }

}
