package com.liquidlabs.log.fields;


import org.junit.Test;
import org.junit.Assert.*;

import static org.junit.Assert.*;

/**
 * Created by neil on 14/03/17.
 */
public class CsvSplitUtilTest {
    @Test
    public void shouldHandleStandardCSV(){
        CsvSplitUtil splitter = new CsvSplitUtil();
        String[] split = splitter.extract("a,b,c");
        assertEquals(split.length, 3);
    }
    

    @Test
    public void standardResultsDontContainCommas(){
        CsvSplitUtil splitter = new CsvSplitUtil();
        String[] split = splitter.extract("a,b,c");
        assertFalse(split[0].contains(","));
        assertFalse(split[1].contains(","));
        assertFalse(split[2].contains(","));
    }

    @Test
    public void shouldHandleEscapedCSV(){
        CsvSplitUtil splitter = new CsvSplitUtil();
        String[] split = splitter.extract("a,\"b,c,d\",e");
        assertEquals(split.length, 3);
    }

    @Test
    public void shouldHandleDefaultConstructor(){
        CsvSplitUtil splitter = new CsvSplitUtil();
        String[] split = splitter.extract("");
        assertNotNull(split);
        assertTrue(split.length == 0);
    }

    @Test
    public void shouldBenchmark(){
        int runCount = 0;

        CsvSplitUtil splitter = new CsvSplitUtil();
        long start = System.currentTimeMillis();

        while(runCount++ < 1000000) {
            splitter.extract("1,\"2,3,4\",5");
        }
        long end = System.currentTimeMillis();
        System.out.println(runCount + " E:" + (end - start));
    }

    @Test
    public void shouldHandleRealWorldData(){
        CsvSplitUtil splitter = new CsvSplitUtil();
        String[] split = splitter.extract("#Record ID , Company Size , Location , What sector of business , Sector_other , Role in organization , ROLE_OTHER , How important is security to management , How well do staff understand your security policy , What percent of IT budget is used on security,  How many times in the past year has your data storage failed or become corrupted , How many times in the past year have you been infected by malicious software , How many times in the past year have staff accessed innapropriate websites , How many times in the past year have staff sent innapropriate emails , How many times in the past year have staff used another users credentials , How many times in the last year have staff broken data protection laws , How many times in the past year have your staff obtained and misused confidential data , How many times in the past year have staff lost or leaked confidential data , How many times have your staff performed financial fraud , How many times in the past year have staff stole equipement , How many times in the past year have staff sabotaged equipment or data , In the past year how many outside attackers have tried to access your network? , How many times in the past year have unauthorised attackers accessed your network , How many times in the past year have unauthorized attackers launched a DDOS attack against your network , How many times in the past year have you recieved a man in the middle attack , How many times in the past year have attackers pretended to be your company , How many times in the past year have attackers pretended to be a client , How many times in the past year have attackers stolen equipment , How many times in the past year have attackers stolen confidential data");

    }
}
