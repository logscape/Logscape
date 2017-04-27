package com.liquidlabs.log.fields;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class FieldGuesser2Test {
	
	@Test
	public void shouldPeelCSVBananas() throws Exception {
		String[] line =  {
				"207.46.12.22, -, -, [31/Jul/2010:09:10:18 -0400], \"GET /plugins/system/rokbox/themes/light/rokbox-config.js HTTP/1.1\", 200, 2598, \"http://www.liquidlabs-cloud.com/products/-notifications.html\", \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2;  SLCC1;  .NET CLR 1.1.4325;  .NET CLR 2.0.50727;  .NET CLR 3.0.30729)\"\n", 
				"207.46.12.22, -, -, [31/Jul/2010:09:10:23 -0400], \"GET /templates/system/css/general.css HTTP/1.1\", 200, 2341, \"http://www.liquidlabs-cloud.com/products/-notifications.html\", \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2;  SLCC1;  .NET CLR 1.1.4325;  .NET CLR 2.0.50727;  .NET CLR 3.0.30729)\"\n", 
				"207.46.12.22, -, -, [31/Jul/2010:09:10:29 -0400], \"GET /templates/rt_hivemind_j15/css/template.css HTTP/1.1\", 200, 13544, \"http://www.liquidlabs-cloud.com/products/-notifications.html\", \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2;  SLCC1;  .NET CLR 1.1.4325;  .NET CLR 2.0.50727;  .NET CLR 3.0.30729)\"\n" 
		};
		String[][] splitLines = new FieldSetGuesser2().getSplitLines(line, ",");
		assertEquals(9, splitLines[0].length);
	}
	@Test
	public void shouldPeelSpaceyBananas() throws Exception {
		String[] line =  {
				"207.46.12.22 - - [31/Jul/2010:09:10:18 -0400] \"GET /plugins/system/rokbox/themes/light/rokbox-config.js HTTP/1.1\" 200 2598 \"http://www.liquidlabs-cloud.com/products/-notifications.html\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2;  SLCC1;  .NET CLR 1.1.4325;  .NET CLR 2.0.50727;  .NET CLR 3.0.30729)\"\n", 
				"207.46.12.22 - - [31/Jul/2010:09:10:23 -0400] \"GET /templates/system/css/general.css HTTP/1.1\" 200 2341 \"http://www.liquidlabs-cloud.com/products/-notifications.html\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2;  SLCC1;  .NET CLR 1.1.4325;  .NET CLR 2.0.50727;  .NET CLR 3.0.30729)\"\n", 
				"207.46.12.22 - - [31/Jul/2010:09:10:29 -0400] \"GET /templates/rt_hivemind_j15/css/template.css HTTP/1.1\" 200 13544 \"http://www.liquidlabs-cloud.com/products/-notifications.html\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2;  SLCC1;  .NET CLR 1.1.4325;  .NET CLR 2.0.50727;  .NET CLR 3.0.30729)\"\n" 
		};
		String[][] splitLines = new FieldSetGuesser2().getSplitLines(line, "\\s+");
		System.out.println(Arrays.toString(splitLines[0]));
		assertEquals(10, splitLines[0].length);
	}
	

}
