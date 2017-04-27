package com.liquidlabs.common.file;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.raf.BreakRuleUtil;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class BreakRuleUtilTest {
	String year = new DateTime().getYear() + "";
	
	@Test
	public void shouldGetGoodYearFromDif() throws Exception {
		String[] lines = new String[] { 
				year + "-04-23 09:21:05.545 : Starting log for Diffusion\r\n" };//+
//				"2012-04-23 09:28:45.892 : WARNING : HTTP Processing error\r\n" + 
//				"Stack Trace:\r\n" + 
//				"com.pushtechnology.diffusion.DiffusionException: Not a Diffusion request [GET /diffusion?t=null&v=3&ty=WB HTTP/1.1\r\n" + 
//				"cache-control : no-cache\r\n" + 
//				"cookie : __utma=238240706.1675132756.1330108160.1334969375.1335167409.7; __utmz=238240706.1334876529.4.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not provided); WT_FPC=id=2deb4671c34ce0384b01330108160480:lv=1335163809369:ss=1335163809369; __utmc=238240706; session=FTL-PRD-DIF01-1nzr3ziw6wf18\r\n" + 
//				"connection : Keep-Alive\r\n" + 
//				"accept-language : pt-pt,pt;q=0.8,en;q=0.5,en-us;q=0.3,x-ns1_CXaBe_4NxM,x-ns2H41MxcVGQb2\r\n" + 
//				"host : d1.sportingbet.com\r\n" + 
//				"accept : text/html,application/xhtml xml,application/xml;q=0.9,*/*;q=0.8\r\n" + 
//				"origin : http://pt.sportingbet.com\r\n" + 
//				"sec-websocket-key : PUqz//NtTNH9DcrMODtPyA==\r\n" + 
//				"user-agent : Mozilla/5.0 (Windows NT 6.1; WOW64; rv:11.0) Gecko/20100101 Firefox/11.0\r\n" + 
//				"accept-encoding : gzip, deflate\r\n" + 
//				"sec-websocket-version : 13\r\n" + 
//				"pragma : no-cache\r\n" + 
//				"]\r\n" + 
//				"                at com.pushtechnology.diffusion.webserver.services.clientservice.ClientHTTPRequestHelper.getCommand(ClientHTTPRequestHelper.java:243)\r\n" + 
//				"                at com.pushtechnology.diffusion.webserver.services.clientservice.ClientService.handleHTTPRequest(ClientService.java:140)\r\n" + 
//				"                at com.pushtechnology.diffusion.webserver.services.clientservice.ClientService.processRequest(ClientService.java:109)\r\n" + 
//				"                at com.pushtechnology.diffusion.webserver.WebServer.processRequest(WebServer.java:91)\r\n" + 
//				"                at com.pushtechnology.diffusion.webserver.WebServerManager.processRequest(WebServerManager.java:68)\r\n" + 
//				"                at com.pushtechnology.diffusion.comms.http.HTTPConnectionHandler.handleInputBuffer(HTTPConnectionHandler.java:145)\r\n" + 
//				"                at com.pushtechnology.diffusion.io.nio.BaseNIOTaskHandler.handleInput(BaseNIOTaskHandler.java:100)\r\n" + 
//				"                at com.pushtechnology.diffusion.io.nio.NIOTask.run(NIOTask.java:152)\r\n" + 
//				"                at com.pushtechnology.diffusion.threads.FastThreadPool$PoolWorker.run(FastThreadPool.java:756)\r\n" + 
//				"                at java.lang.Thread.run(Unknown Source)\r\n" + 
//				" \r\n" 
		//};
		String rule = BreakRuleUtil.getStandardNewLineRule(Arrays.asList(lines), "default", "");
		assertEquals("Year",rule);
	}

    @Test
    public void shouldGrabYearFromCorr() throws Exception {
        String[] lines = new String[] {
                "21-feb-2014 13:45:35.442 INFO: [createContactHistorie] [OUT-RESP] [USER = ExPartySalesForcePRD :: AILHEADER/CLIENTID = Salesforc-2014 13:45:36.044 INFO: [listContactHistorie] [IN-REQ] [USER = appS3CAGEON :: AILHEADER/CLIENTID = Ceesiebo :: AILHEADER/CORRELATIONID = ##BS_AE_Contact_Index##Fri Feb 21 13:45:36 GMT 2014## :: DOSSIER/CONTRACT_POLIS/NUMMER = null :: DOSSIER/CONTRACT_POLIS/PRODUCT = null :: DOSSIER/CONTRACT_POLIS/_AE_BRONSYSTEEM = null]"
        };
        String rule = BreakRuleUtil.getStandardNewLineRule(Arrays.asList(lines), "default", "");
        assertEquals("DayNumeric",rule);
    }




    @Test
	public void shouldGrabYearFromGCLogs() throws Exception {
		String[] lines = new String[] { 
				year + "-04-07T08:30:21.707+0100: 321611.501: [GC 321611.502: [ParNew: 218903K->7935K(235968K), 0.0125940 secs] 458141K->251922K(2070976K), 0.0126920 secs] [Times: user=0.04 sys=0.01, real=0.02 secs]",
				year + "-04-07T09:21:57.848+0100: 324707.642: [GC 324707.643: [ParNew: 217727K->7305K(235968K), 0.0032180 secs] 461714K->251293K(2070976K), 0.0032880 secs] [Times: user=0.02 sys=0.00, real=0.01 secs]",
				year + "-04-07T10:13:40.291+0100: 327810.085: [GC 327810.086: [ParNew: 217097K->7704K(235968K), 0.0038340 secs] 461085K->251691K(2070976K), 0.0039160 secs] [Times: user=0.02 sys=0.00, real=0.00 secs]",
				year + "-04-07T11:04:53.374+0100: 330883.168: [GC 330883.168: [ParNew: 217496K->8277K(235968K), 0.0079850 secs] 461483K->253976K(2070976K), 0.0080670 secs] [Times: user=0.03 sys=0.01, real=0.01 secs]"
		};
		String rule = BreakRuleUtil.getStandardNewLineRule(Arrays.asList(lines), "default", "");
		assertEquals("Year",rule);
	}
	@Test
	public void detectedNothingCorrectly() throws Exception {
		String[] lines = new String[] { "0124/12/2010 09:20:56 AM  murex.apps.business.server.limits.MxLog INFO {Message=Starting MLC Service ...}",
						" : 24/12/2010 09:20:56 AM  murex.apps.business.server.limits.MxLog INFO {Message=strArgs : /MXJ_MLC_PROPS_FILE:public.mxres.mxmlc.mlc.mxres}",
						" : 24/12/2010 09:20:56 AM  murex.apps.business.server.limits.MxLog INFO {Message=propsFilePath : public.mxres.mxmlc.mlc.mxres}",
						" : 24/12/2010 09:20:56 AM  murex.apps.business.server.limits.MxLog INFO {Message=strArgs : /MXJ_MLC_PLUGINS_FILE:public.mxres.mxmlc.plugins.mxres}",
						" : 24/12/2010 09:20:56 AM  murex.apps.business.server.limits.MxLog INFO {Message=strArgs : /MXJ_MLC_REMOTE:N}",
						" : 24/12/2010 09:20:56 AM  murex.apps.business.server.limits.MxLog INFO {Message=strArgs : /MPEXPCONFIG:MY_MPEXP_CONFIG}"};
		String rule = BreakRuleUtil.getStandardNewLineRule(Arrays.asList(lines), "default", "");
		assertEquals("default",rule);
	}
	
	@Test
	public void detectsYear() throws Exception {
		List<String> content = Arrays.asList("crap", year + "/03/01 some log line");
		String rule = BreakRuleUtil.getStandardNewLineRule(content, "default", "");
		assertEquals("Year",rule);
	}
	
	@Test
	public void detectsMonth() throws Exception {
		List<String> content = Arrays.asList("crap", "Jan/01/2011 some log line");
		String rule = BreakRuleUtil.getStandardNewLineRule(content, "default", "");
		assertEquals("Month",rule);
	}
	@Test
	public void detectsDayNumeric() throws Exception {
		List<String> content = Arrays.asList("crap", "01/01/2011 some log line");
		String rule = BreakRuleUtil.getStandardNewLineRule(content, "default", "");
		assertEquals("DayNumeric",rule);
	}
	@Test
	public void detectsMonthHardNumeric() throws Exception {
		List<String> content = Arrays.asList("01/31/2011 some log line","01/31/2011");
		String rule = BreakRuleUtil.getStandardNewLineRule(content, "default", "");
		assertEquals("MonthNumeric",rule);
	}


}
