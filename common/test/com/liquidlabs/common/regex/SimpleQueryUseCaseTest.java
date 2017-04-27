package com.liquidlabs.common.regex;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

public class SimpleQueryUseCaseTest {
	
	SimpleQueryConvertor convertor = new SimpleQueryConvertor();
	
	@Test
	public void shouldHandleTabDelimit() throws Exception {
		String token = "\t";
		String expr = "(*" + token + ")\t(*"+token+")";
		String regexp = convertor.convertSimpleToRegExp(expr);
		assertTrue("this\tthis".matches(regexp));
	}
	
	@Test
	public void shouldConvert2ndGroupWithNotScrewingWhiteSpace() throws Exception {
		String expr = "From * to (*)";
		String regexp = convertor.convertSimpleToRegExp(expr);
		// didnt match cause it lost the whitespace before the (
		System.out.println("RegExp:" + regexp);
//		RegExpUtil.matches(regexp, "From AAAA to BBB");
		MatchResult matches = RegExpUtil.matches(regexp, "Large ICMP packet! From AAA.BBB.CCC.DDD to AAA.BBB.CCC.EEE, proto 1 (zone V1-Untrust, int v1-untrust). Occurred 1 times. (2006-04-05 14:35:14)");
		System.out.println(Arrays.toString(matches.groups));
		assertTrue(matches.groups() > 0);
		
	}
	
	@Test
	public void shouldHandleExcludeFiltersOnTabChar() throws Exception {
		String expression = "^(w)\\t(^\\t)\\t(w)";
		String line = "one	twoA twoB	three";
		String regexp = convertor.convertSimpleToRegExp(expression);
		System.out.println("RE:" + regexp);
		MatchResult matches = RegExpUtil.matches(regexp, line);
		
		assertTrue(matches.isMatch());
		assertEquals(Arrays.toString(matches.groups), 4, matches.groups.length);

		
	}
	
//	@Test

    /**
     * No idea what this test is trying to achieve because the regex and what tying to match are never going to.
     * @throws Exception
     */
	public void shouldDoStarBracketGroup() throws Exception {
		String line = "14:53:16 drop gw.foobar.com >eth0 product VPN-1 & Firewall-1 src xxx.xxx.146.12 s_prt 2523 dst xxx.xxx.10.2 service ms-sql-m proto udp rule 49";
		
//		String expression = "(*[)";
		String expression = "^(\\S+ \\S+)\\s+\\[Origin:(.*?), Mandant:(.*?), Instance:(.*?), User:(.*?), IP:(.*?), APP-Server:(.*?)\\] (.*?)\\] ([^\\[\\]]+)\\[(.*)";
		
		String regexp = convertor.convertSimpleToRegExp(expression);
		System.out.println(regexp);
		MatchResult matchesJava = RegExpUtil.matchesJava(regexp, "some line [ stuff");
		assertTrue(matchesJava.isMatch());

	}
	@Test
	public void shouldDoStarStarGroup() throws Exception {
		String line = "14:53:16 drop gw.foobar.com >eth0 product VPN-1 & Firewall-1 src xxx.xxx.146.12 s_prt 2523 dst xxx.xxx.10.2 service ms-sql-m proto udp rule 49";
		
		String expression = "^(*) (w) (*) (*) (**) src (*) (w) (w) (dst) (*) (w) (*) (proto) (w) (rule) (w)";
		
		String regexp = convertor.convertSimpleToRegExp(expression);
		
		MatchResult matches = RegExpUtil.matches(regexp, line);
		
		assertTrue(matches.isMatch());
		System.out.println(regexp);
	}
	@Test
	public void shouldMatchIPAddress() throws Exception {
		String line = "14:53:16 192.168.70.8 stuff";
		String expression = "(*) (192.d.d.d)";
		String regexp = convertor.convertSimpleToRegExp(expression);
		MatchResult matches = RegExpUtil.matches(regexp, line);
		
		assertTrue("Failed to match, expression:" + regexp, matches.isMatch());
	}
	
	@Test
	public void shouldDoLogSpaceCPULine() throws Exception {
		String line = "Cache: Current size, allocations, and eviction activity.\n" + 
				"	adminBytes=1,885\n" + 
				"	cacheTotalBytes=4,643,212\n" + 
				"	dataBytes=1,495,119";
		
//		String expression = "AGENT (*)-(d) AND COMMITED:(d) AND AVAIL:(d)";
		String expression = "Cache\\nadminBytes=(*)\\ncacheTotalBytes=(*)\\ndataBytes=(*)";
		
//		String expression = "\\((*).(*)\\)";
		
		
		String regexp = convertor.convertSimpleToRegExp(expression);
		System.out.println("e:" + regexp);
		MatchResult matches = RegExpUtil.matches(regexp, line);
		System.out.println("Line:\t" + line);
		System.out.println("Expr:\t" + regexp);
		System.out.println("Match:" + matches);
		assertTrue(matches.match);
		assertEquals(4, matches.groups.length);
	}
	
	@Test
	public void synthSearchIsGood() throws Exception {
		String expression = "SEARCH[(*)]";
		String regexp = convertor.convertSimpleToRegExp(expression);
		System.out.println(expression);
		System.out.println(regexp);
		assertEquals(".*?SEARCH\\[(\\S+)\\].*", regexp);
	}
	
	@Test
	public void shouldNotBreakCPURegexp() throws Exception {
		String expression = ".*AGENT (.*)-\\d+ CPU:(\\d+).*";
		String regexp = convertor.convertSimpleToRegExp(expression);
		System.out.println(expression);
		System.out.println(regexp);
		assertEquals(expression, regexp);
	}
	
	@Test
	public void shouldExtractPerformanceOnDELIM() throws Exception {
		String text = "2010/07/09 23:59:59|datapair|info|119de7b35985488|bzo|43935fe0-c66c-412c-9396-d43f0913ea40|rep||G8K,U2A,I8T,M4F,A1N,KGES6N";
		String expression = "^(*) (*)|(*)|(*)|(*)|(*)|(*)|(*)|(*)";
		// convert into 
	//	String expects = ".*size=(\\w+).*";
		
		String regExp = convertor.convertSimpleToRegExp(expression);
		
		System.out.println("rrrrrrrrr:" + regExp);
		
		MatchResult matches = RegExpUtil.matches(regExp, text);
		assertTrue("Got bad regexp: " + regExp, matches.isMatch());
		//assertEquals("728x90",matches.groups[1]);			
	}
	@Test
	public void shouldExtractMultiwordCityNameWithW() throws Exception {
		String text = "2010-07-08 11:18:52 -0500: rtb|CAESEEBQdg_vSMQb6WrhuJOVl7c_1|1172c8592184239|bet|ad=226623210,price=450000,size=728x90,segments=cm.fash_L,cm.soccer_L,cm.aa_ddh,cm.ent,cm.ent_L,cm.gadg_L,cm.tech_L,cm.polit_L,cm.sports_L,city=Palisades Park,country=US,region=US-NJ,metro=501";
		String expression = "size=(w)";
		// convert into 
		String expects = ".*size=(\\w+).*";
		
		String regExp = convertor.convertSimpleToRegExp(expression);
		
		System.out.println("rrrrrrrrr:" + regExp);
		
		MatchResult matches = RegExpUtil.matches(regExp, text);
		assertTrue("Got bad regexp: " + regExp, matches.isMatch());
		assertEquals("728x90",matches.groups[1]);			
	}
	@Test
	public void shouldExtractMultiwordCityName() throws Exception {
		String text = "2010-07-08 11:18:52 -0500: rtb|CAESEEBQdg_vSMQb6WrhuJOVl7c_1|1172c8592184239|bet|ad=226623210,price=450000,size=728x90,segments=cm.fash_L,cm.soccer_L,cm.aa_ddh,cm.ent,cm.ent_L,cm.gadg_L,cm.tech_L,cm.polit_L,cm.sports_L,city=Palisades Park,country=US,region=US-NJ,metro=501";
		String expression = "city=(*,)";
		// convert into 
		String expects = ".*?city=([^,]+).*";
		
		String regExp = convertor.convertSimpleToRegExp(expression);
		
		MatchResult matches = RegExpUtil.matches(regExp, text);
		assertTrue("Got bad regexp: " + regExp, matches.isMatch());
		assertEquals(expects, regExp);
		
	}
	
	@Test
	public void shouldWorkWithCOLLECTIVE_Pipes() throws Exception {
		String text = "2010/06/16 16:16:27|datapair|info|1196651b06098a7|sjrn|1196651b06098a7|blang|p1=usair|d_kphx,o_klga,c_c,b_0,l_,p_3,u_6more,r_1,g_na,v_dd,a_na,x_0,w_4,m_na,f_na,t_na,e_na,y_na,q_A,h_k,i_k,j_kl,k_kp,pr_na,s_2610,re_0,z_usair";
		
		//Here’s a typical log file where I’d first narrow the search to records containing “datapair” 
		// and having a particular value of net=<value> (e.g. net=na)
		String expression = "(*) (*)|(*)|(*)|(*)|(*)|(*)|(*)|(*)|(*)";
		String regExp = convertor.convertSimpleToRegExp(expression);
		System.out.println("Got:" + regExp);
		MatchResult matches = RegExpUtil.matches(regExp, text);
		System.out.println(matches);
		assertTrue(matches.isMatch());
		
	}
	@Test
	public void shouldWorkWithCOLLECTIVE_DataPair() throws Exception {
		String text = "10.24.154.78 - - [16/Jun/2010:08:43:40 -0500] GET /datapair?net=na&id=2010051116005452092621124345&pid=507&segs=,TX197_T,TX280_T,A20_Y,A22_Y,A57_Y, HTTP/1.1 request_time 0.003";
		
		//Here’s a typical log file where I’d first narrow the search to records containing “datapair” 
		// and having a particular value of net=<value> (e.g. net=na)
		String expression = "(net)=(*)&(id=*)&(pid=*)&(segs)=(*)";
		String regExp = convertor.convertSimpleToRegExp(expression);
		System.out.println("Got:" + regExp);
		MatchResult matches = RegExpUtil.matches(regExp, text);
		System.out.println(matches);
		assertTrue(matches.isMatch());
		
	}

}
