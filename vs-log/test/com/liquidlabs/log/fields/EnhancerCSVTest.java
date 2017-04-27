package com.liquidlabs.log.fields;

import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Created by neil on 15/07/2015.
 */
public class EnhancerCSVTest {

    @Test
    public void testEnhanceAgain() throws Exception {

        String[] data = ("\"number\",\"mode\",\"price\",\"catch\",\"pbeach\",\"ppier\",\"pboat\",\"pcharter\",\"cbeach\",\"cpier\",\"cboat\",\"ccharter\",\"income\"\n" +
                "\"1\",\"charter\",182.93,0.5391,157.93,157.93,157.93,182.93,0.0678,0.0503,0.2601,0.5391,7083.3317\n" +
                "\"2\",\"charter\",34.534,0.4671,15.114,15.114,10.534,34.534,0.1049,0.0451,0.1574,0.4671,1249.9998").split("\n");
        FieldSet basic = FieldSets.getBasicFieldSet();
        FieldSet csvd = EnhancerCSV.createIt(basic, "boo.csv", Arrays.asList(data));
        String[] fields = csvd.getFields(data[1]);
        assertThat("charter", is(equalTo(csvd.getFieldValue("mode", fields))));
    }

    @Test
    public void testEnhance() throws Exception {

        String[] data = ("Transaction_date,Product,Price,Payment_Type,Name,City,State,Country,Account_Created,Last_Login,Latitude,Longitude\n" +
                "1/2/09 6:17,Product1,1200,Mastercard,carolina,Basildon,England,United Kingdom,1/2/09 6:00,1/2/09 6:08,51.5,-1.1166667\n" +
                "1/2/09 4:53,Product1,1200,Visa,Betina,Parkville                   ,MO,United States,1/2/09 4:42,1/2/09 7:49,39.195,-94.68194\n" +
                "1/2/09 13:08,Product1,1200,Mastercard,Federica e Andrea,Astoria                     ,OR,United States,1/1/09 16:21,1/3/09 12:32,46.18806,-123.83\n" +
                "1/3/09 14:44,Product1,1200,Visa,Gouya,Echuca,Victoria,Australia,9/25/05 21:13,1/3/09 14:22,-36.1333333,144.75\n" +
                "1/4/09 12:56,Product2,3600,Visa,Gerd W ,Cahaba Heights              ,AL,United States,11/15/08 15:47,1/4/09 12:45,33.52056,-86.8025\n" +
                "1/4/09 13:19,Product1,1200,Visa,LAURENCE,Mickleton                   ,NJ,United States,9/24/08 15:19,1/4/09 13:04,39.79,-75.23806\n").split("\n");
        FieldSet basic = FieldSets.getBasicFieldSet();
        FieldSet csvd = EnhancerCSV.createIt(basic, "boo.csv", Arrays.asList(data));
        String[] fields = csvd.getFields(data[1]);
        assertThat("Mastercard", is(equalTo(csvd.getFieldValue("Payment_Type", fields))));
    }
}