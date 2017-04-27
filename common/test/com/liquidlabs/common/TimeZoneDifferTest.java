package com.liquidlabs.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import org.junit.Assert;
import org.junit.Test;

public class TimeZoneDifferTest {

	
	@Test
	public void shouldWorkGood() throws Exception {
		String[] availableIDs = TimeZone.getAvailableIDs();
		List<String> tzl = new ArrayList<String>(Arrays.asList(availableIDs));
		Collections.sort(tzl);
		for (String tz : tzl) {
			System.out.println(tz);
		}
		System.out.println("CurrentTimeZome:" + TimeZone.getDefault().getID());
		
		int hoursDiff = TimeZoneDiffer.getHoursDiff("GMT+0");
		System.out.println(hoursDiff);
		Assert.assertTrue(hoursDiff == 0 || hoursDiff == 1);
		
	}
}
