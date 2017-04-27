package com.liquidlabs.common.expr;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Match:
 * (2*)\s+(*)\s+(INFO|DEBUG|WARN|ERROR|FATAL)\s+(*)\s+(*)\s+(**)
 * (* * *)\s+(*)\s+(**)
 * Maybe a keyvalue
 * (*) "(*):" "(*)",(**)
 *
  IIS


 *  Squid, Win2008-Event,Apache
 * */

public class BlockBulderTest {


    @Test
    public void testShouldMatchIIS() {
        //(*\s+*)\s+(*)\s+(*)\s+(\d+\.\d+\.\d+\.\d+)\s+(w)\s+(*)\s+(*)\s+(\d+)\s+(*)\s+(\d+\.\d+\.\d+\.\d+)\s+(*)\s+(*)\s+(*)\s+(*)\s+(*)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(**)
        // header
        // #date time s-sitename s-computername s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs-version csUser-Agent csCookie csReferer cs-host sc-status sc-substatus sc-win32-status sc-bytes cs-bytes time-taken
        // 2012-07-18 23:59:59 W3SVC1659890597 COM-EN-W02 10.201.3.68 GET /MarketTemplate.mvc/GetCoupon couponAction=EVALLMARKETS&amp;sportIds=102&amp;marketTypeId=&amp;eventId=2062449&amp;bookId=&amp;eventClassId=84042&amp;sportId=0&amp;oddsFormat=EU&amp;eventTimeGroup= 8067 - 195.178.6.12 HTTP/1.0 Mozilla/5.0+(Windows+NT+6.1;+WOW64;+rv:11.0)+Gecko/20100101+Firefox/11.0 language-com-en=FOUND;+language-com-en=FOUND - www.sportingbet.com 200 0 0 1889 815 46
        // 2012-07-18 23:59:59 W3SVC1659890597 COM-EN-W02 10.201.3.68 GET /InPlayApp.mvc/GetMarketGroups eventId=2069911&amp;oddsFormat=EU 8067 - 195.178.6.12 HTTP/1.0 Mozilla/5.0+(compatible;+MSIE+9.0;+Windows+NT+6.0;+Trident/5.0) op699betslipmodulegum=a04m08e06q287ht0md3vo8287iu0o02av9116;+op699betslippostbetgum=a04x08n07s287it0nd1490287iv0oc24d55d7;+op699idolsearchgum=a04k08c07a287gu0870df6287iv0or23m45a9;+op699http___www_sportingb_6gum=a04306n05m287gu08377b0287iu0o22ahf3bd;+__utma=238240706.1925438800.1342435930.1342649969.1342650463.4;+__utmz=238240706.1342649969.3.3.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=sportingbet;+opStartTime=1342112836378;+__utmc=238240706;+op699betslipmoduleliid=a04m08e06q287ht0md3vo8287iu0o02av9116;+op699betslippostbetliid=a04x08n07s287it0nd1490287iv0oc24d55d7;+op699idolsearchliid=a04k08c07a287gu0870df6287iv0or23m45a9;+op699http___www_sportingb_6liid=a04306n05m287gu08377b0287iu0o22ahf3bd;+__utmb=238240706.24.9.1342655662682;+language-com-en=FOUND;+rStatus=3;+ili=yes;+__utma=1.1671253775.1342435930.1342649969.1342650463.4;+__utmz=1.1342649969.3.3.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=sportingbet;+SB_Rewards_Status_UpdateDate=2012-7-19;+SB_Rewards_Status_Optin=true;+SB_Rewards_Status_Balance=4717.0;+tutorialwelcome=1;+ASP.NET_SessionId=o1nfp155wy151p554espc445;+__utmc=1;+__g_c=w%3A1%7Cb%3A150%7Cc%3A297256742028812%7Cd%3A1%7Ca%3A0%7Ce%3A0.1%7Cf%3A0%7Ch%3A1;+affiliate=;+promotion=;+OddsFormat=EU;+startTime=5319;+lpCloseInvite=null;+__utmb=1.18.10.1342650463;+searchOddsFormat=EU https://www.sportingbet.com/sports-bet-in-play/5.html?linkid=Sport-LHN www.sportingbet.com 200 0 0 7829 1967 78


    }
    @Test
    public void testShouldMatchSQUID() {
        //(*\s+*)\s+(*)\s+(*)\s+(\d+\.\d+\.\d+\.\d+)\s+(w)\s+(*)\s+(*)\s+(\d+)\s+(*)\s+(\d+\.\d+\.\d+\.\d+)\s+(*)\s+(*)\s+(*)\s+(*)\s+(*)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(**)
        // header
        // #date time s-sitename s-computername s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs-version csUser-Agent csCookie csReferer cs-host sc-status sc-substatus sc-win32-status sc-bytes cs-bytes time-taken
        // 2012-07-18 23:59:59 W3SVC1659890597 COM-EN-W02 10.201.3.68 GET /MarketTemplate.mvc/GetCoupon couponAction=EVALLMARKETS&amp;sportIds=102&amp;marketTypeId=&amp;eventId=2062449&amp;bookId=&amp;eventClassId=84042&amp;sportId=0&amp;oddsFormat=EU&amp;eventTimeGroup= 8067 - 195.178.6.12 HTTP/1.0 Mozilla/5.0+(Windows+NT+6.1;+WOW64;+rv:11.0)+Gecko/20100101+Firefox/11.0 language-com-en=FOUND;+language-com-en=FOUND - www.sportingbet.com 200 0 0 1889 815 46
        // 2012-07-18 23:59:59 W3SVC1659890597 COM-EN-W02 10.201.3.68 GET /InPlayApp.mvc/GetMarketGroups eventId=2069911&amp;oddsFormat=EU 8067 - 195.178.6.12 HTTP/1.0 Mozilla/5.0+(compatible;+MSIE+9.0;+Windows+NT+6.0;+Trident/5.0) op699betslipmodulegum=a04m08e06q287ht0md3vo8287iu0o02av9116;+op699betslippostbetgum=a04x08n07s287it0nd1490287iv0oc24d55d7;+op699idolsearchgum=a04k08c07a287gu0870df6287iv0or23m45a9;+op699http___www_sportingb_6gum=a04306n05m287gu08377b0287iu0o22ahf3bd;+__utma=238240706.1925438800.1342435930.1342649969.1342650463.4;+__utmz=238240706.1342649969.3.3.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=sportingbet;+opStartTime=1342112836378;+__utmc=238240706;+op699betslipmoduleliid=a04m08e06q287ht0md3vo8287iu0o02av9116;+op699betslippostbetliid=a04x08n07s287it0nd1490287iv0oc24d55d7;+op699idolsearchliid=a04k08c07a287gu0870df6287iv0or23m45a9;+op699http___www_sportingb_6liid=a04306n05m287gu08377b0287iu0o22ahf3bd;+__utmb=238240706.24.9.1342655662682;+language-com-en=FOUND;+rStatus=3;+ili=yes;+__utma=1.1671253775.1342435930.1342649969.1342650463.4;+__utmz=1.1342649969.3.3.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=sportingbet;+SB_Rewards_Status_UpdateDate=2012-7-19;+SB_Rewards_Status_Optin=true;+SB_Rewards_Status_Balance=4717.0;+tutorialwelcome=1;+ASP.NET_SessionId=o1nfp155wy151p554espc445;+__utmc=1;+__g_c=w%3A1%7Cb%3A150%7Cc%3A297256742028812%7Cd%3A1%7Ca%3A0%7Ce%3A0.1%7Cf%3A0%7Ch%3A1;+affiliate=;+promotion=;+OddsFormat=EU;+startTime=5319;+lpCloseInvite=null;+__utmb=1.18.10.1342650463;+searchOddsFormat=EU https://www.sportingbet.com/sports-bet-in-play/5.html?linkid=Sport-LHN www.sportingbet.com 200 0 0 7829 1967 78


    }
    @Test
    public void testShouldMatchLOG4J() {
        //(2*)\s+(*)\s+(INFO|DEBUG|WARN|ERROR|FATAL)\s+(*)\s+(*)\s+(**)
        // header
        // #date time level thread warn package msg
        //
        String line = "2010-09-04 11:56:23,422 WARN main (vso.SpaceServiceImpl) LogSpaceBoot All Available Addresses:[ServiceInfo name[AdminSpace] zone[stcp://192.168.0.2:11013?";
        //Jul 31 01:45:00 10.162.0.25 TP-Processor951 ERROR handlers.AbstractRestHandler error.159  - Exception occurred.  Reason Code: SERVER_DOES_NOT_EXIST.  Reason Detail: Could not find server with Id e2818271-f8f0-4959-919c-03c5cd7d0f58 : Text logged: Could not find Server with Id e2818271-f8f0-4959-919c-03c5cd7d0f58
        List<Block> build = BlockBuilder.build("(*) (*) (Aa+) (Aa+) (*) (**)");

        long start = System.currentTimeMillis();
        //for (int i = 0; i < 2 * 2 * 500 * 1000; i++) {
        for (int i = 0; i < 500 * 1000; i++) {
            List<String> results = BlockBuilder.match(build,line);
        }
        long end = System.currentTimeMillis();
        System.out.println("E:" + (end -start));
        // does 2million/second
        //assertTrue(results.size() > 0);



    }


    @Test
    public void shouldAlphaTEXTParse() throws Exception {
        List<Block> build = BlockBuilder.build("(A1+) (**)");
        List<String> results = BlockBuilder.match(build,"PASS100 and two");
        assertEquals("[PASS100, and two]", results.toString());
        assertNull(BlockBuilder.match(build,"f!ail and two"));
        assertNull(BlockBuilder.match(build,"http://fail and two"));
    }

    @Test
    public void shouldTEXTParse() throws Exception {
        List<Block> build = BlockBuilder.build("(A+) (**)");
        List<String> results = BlockBuilder.match(build,"PASS and two");
        assertEquals("[PASS, and two]", results.toString());
        assertNull(BlockBuilder.match(build,"Afail and two"));
    }


    @Test
    public void shouldBasicParseWithDelimiter() throws Exception {
        List<Block> build = BlockBuilder.build("(*)|(A+)|(**)");
        List<String> results = BlockBuilder.match(build,"A|B|C");
        assertEquals("[A, B, C]", results.toString());
    }

    @Test
    public void shouldBasicParse() throws Exception {
        List<Block> build = BlockBuilder.build("(*) (**)");
        assertEquals("Got:" + build,2, build.size());
    }
    @Test
    public void shouldGetGroupsOnMatch() throws Exception {
        List<Block> build = BlockBuilder.build("(*) (**)");
        List<String> results = BlockBuilder.match(build,"section1 and the rest");
        assertEquals("[section1, and the rest]", results.toString());
    }


    @Test
    public void shouldGetMultipleGroupsOnMatch() throws Exception {
        List<Block> build = BlockBuilder.build("(*) (A+) (A1+) (*) (**)");
        long start = System.currentTimeMillis();
        int count = 0;

        for (int i = 0; i < 1000 * 1000; i++) {
            List<String> results = BlockBuilder.match(build,"one TWO three3 four five and stuff");
            count++;
        }
        long end = System.currentTimeMillis();
        System.out.println(count + " Elapsed:" + (end - start));
//        assertEquals("[one, two, three, four, five and stuff]", results.toString());
    }






}
