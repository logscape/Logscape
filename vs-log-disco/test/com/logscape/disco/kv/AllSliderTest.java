package com.logscape.disco.kv;

import org.junit.Test;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;


/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 08/01/2015
 * Time: 11:05
 * To change this template use File | Settings | File Templates.
 */
public class AllSliderTest {

    @Test
    public void testShouldDetectNumbers() throws Exception {

        AllSlidersHandler slider = new AllSlidersHandler();


        assertFalse(slider.isNumber("zz"));

        assertFalse(slider.isNumber("zaz"));

        assertFalse(slider.isNumber("aaaz"));
        assertTrue(slider.isNumber("aa1a"));
        assertTrue(slider.isNumber("223.3"));
        assertTrue(slider.isNumber("223"));
        assertTrue(slider.isNumber("1a2e1f"));







    }
}
