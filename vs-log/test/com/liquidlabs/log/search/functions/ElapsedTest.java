package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;


public class ElapsedTest {
    private Elapsed elapsed;
    private FieldSet fieldSet;
	private String rawLineData;
	private String labelGroup;
	private long requestStartTimeMs;
	
	
	@Test
	public void shouldDoIsRunningProperly() throws Exception {
		DateTime now = new DateTime();
		
		ElapsedInfo elapsedInfo = new ElapsedInfo(now.getMillis()+1, now.getMillis()+2,60,"XXX");
		assertTrue(elapsedInfo.isRunning(now.getMillis(), now.plusMinutes(1).getMillis()));
		assertFalse(elapsedInfo.isRunning(now.getMillis(), now.getMillis()));
		
	}

    @Test
    public void shouldHaveOneResult() throws Exception {
        elapsed.execute(fieldSet, fieldSet.getFields("stuff start"), "pattern", 1L, null, rawLineData, requestStartTimeMs, 1);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff end"), "pattern", 10L, null, rawLineData, requestStartTimeMs, 2);
        Map results = elapsed.getResults();
        assertEquals(1, results.size());
    }

    @Test
    public void shouldHavePopulateWithNumericLabelGroup() throws Exception {
    	String labelGroup = "1";
        elapsed = new Elapsed("task", "msg", "start", "end", "k", labelGroup, false);
        fieldSet = new FieldSet("^(\\w+) (.*)", "foo", "msg");
        
        

        elapsed.execute(fieldSet, fieldSet.getFields("stuff start"), "pattern", 1L, new MatchResult(new String[] { "foot start", "foo", "start" } ), rawLineData, requestStartTimeMs, 1);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff end"), "pattern", 10L, new MatchResult(new String[] { "foot end", "foo", "end" } ), rawLineData, requestStartTimeMs, 2);
        Map<String, List<ElapsedInfo>> results = elapsed.getResults();
        ElapsedInfo elapsedTime = results.get("elapsed").get(0);
        assertEquals("foo", elapsedTime.getLabel());
    }
    
    @Test
    public void shouldHavePopulateWithFieldLabelGroup() throws Exception {
    	String labelGroup = "foo";
    	elapsed = new Elapsed("task", "msg", "start", "end", "k", labelGroup, false);
    	fieldSet = new FieldSet("^(\\w+) (.*)", "foo", "msg");
    	
    	elapsed.execute(fieldSet, fieldSet.getFields("stuff start"), "pattern", 1L, null, rawLineData, requestStartTimeMs, 1);
    	elapsed.execute(fieldSet, fieldSet.getFields("stuff end"), "pattern", 10L, null, rawLineData, requestStartTimeMs, 2);
    	Map<String, List<ElapsedInfo>> results = elapsed.getResults();
    	ElapsedInfo elapsedTime = results.get("elapsed").get(0);
    	assertEquals("stuff", elapsedTime.getLabel());
    }
    @Test
    public void shouldHaveElapsedTimeInResult() throws Exception {
    	elapsed.execute(fieldSet, fieldSet.getFields("stuff start"), "pattern", 1L, null, rawLineData, requestStartTimeMs, 1);
    	elapsed.execute(fieldSet, fieldSet.getFields("stuff end"), "pattern", 10L, null, rawLineData, requestStartTimeMs, 2);
    	Map<String, List<ElapsedInfo>> results = elapsed.getResults();
    	ElapsedInfo elapsedTime = results.get("elapsed").get(0);
    	assertThat(elapsedTime.getDuration(), is(9.0));
    }

    @Test
    public void shouldHaveNoElapsedTimesIfOnlyStart() throws Exception {
        elapsed.execute(fieldSet, fieldSet.getFields("stuff start"), "pattern", 1L, null, rawLineData, requestStartTimeMs, 1);
        Map<String, List<ElapsedInfo>> results = elapsed.getResults();
        assertEquals(0, results.get("elapsed").size());
    }

    @Test
    public void shouldHaveNoElapsedTimesIfOnlyEnd() throws Exception {
        elapsed.execute(fieldSet, fieldSet.getFields("stuff end"), "pattern", 1L, null, rawLineData, requestStartTimeMs, 1);
        Map<String, List<ElapsedInfo>> results = elapsed.getResults();
        assertEquals(0, results.get("elapsed").size());
    }

    @Test
    public void shouldHaveElapsedTimeForEachStartEndPair() throws Exception {
        elapsed.execute(fieldSet, fieldSet.getFields("stuff start"), "pattern", 1L, null, rawLineData, requestStartTimeMs, 1);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff end"), "pattern", 10L, null, rawLineData, requestStartTimeMs, 2);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff start"), "pattern", 25L, null, rawLineData, requestStartTimeMs, 3);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff end"), "pattern", 50L, null, rawLineData, requestStartTimeMs, 4);
        Map<String, List<ElapsedInfo>> results = elapsed.getResults();
        assertEquals(2, results.get("elapsed").size());
        assertThat(results.get("elapsed").get(0).getDuration(), is(9.0));
        assertThat(results.get("elapsed").get(1).getDuration(), is(25.0));
    }

    @Test
    public void shouldHandleStartAndEndTheSame() throws Exception {

        elapsed = new Elapsed("task", "msg", "start", "end", "k", labelGroup, false);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff start"), "pattern", 1L, null, rawLineData, requestStartTimeMs, 1);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff end"), "pattern", 10L, null, rawLineData, requestStartTimeMs, 2);

        Map<String, List<ElapsedInfo>> results = elapsed.getResults();
        ElapsedInfo elapsedTime = results.get("elapsed").get(0);
        assertThat(elapsedTime.getDuration(), is(9.0));
    }

    @Test
    public void shouldWorkWithGroup() throws Exception {
        MatchResult start = new MatchResult(new String[]{"stuff start foo", "start foo"});
        MatchResult end = new MatchResult(new String[]{"stuff end foo", "end foo"});
        elapsed = new Elapsed("task", "1", "start", "end", "k", labelGroup, false);

        elapsed.execute(fieldSet, fieldSet.getFields("stuff foo"), "pattern", 1L, start, rawLineData, requestStartTimeMs, 1);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff foo"), "pattern", 10L, end, rawLineData, requestStartTimeMs, 2);

        Map<String, List<ElapsedInfo>> results = elapsed.getResults();
        ElapsedInfo elapsedTime = results.get("elapsed").get(0);
        assertThat(elapsedTime.getDuration(), is(9.0));
    }

    @Test
    public void shouldDisplayAsSecondsWhenSisDisplayUnit() throws Exception {
        elapsed = new Elapsed("task", "msg", "start", "end", "s", labelGroup, false);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff start"), "pattern", 0L, null, rawLineData, requestStartTimeMs, 1);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff end"), "pattern", 1000L, null, rawLineData, requestStartTimeMs, 2);

        Map<String, List<ElapsedInfo>> results = elapsed.getResults();
        ElapsedInfo elapsedTime = results.get("elapsed").get(0);
        assertThat(elapsedTime.getDuration(), is(1.0));
    }

    @Test
    public void shouldHaveStartAndEndTimeInResults() throws Exception {
        elapsed = new Elapsed("task", "msg", "start", "end", "s", labelGroup, false);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff start"), "pattern", 1000L, null, rawLineData, requestStartTimeMs, 1);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff end"), "pattern", 2000L, null, rawLineData, requestStartTimeMs, 2);

        Map<String, List<ElapsedInfo>> results = elapsed.getResults();
        ElapsedInfo elapsedTime = results.get("elapsed").get(0);
        assertEquals(1000L, elapsedTime.getStartTime());
        assertEquals(2000L, elapsedTime.getEndTime());
    }

    @Test
    public void shouldDisplayInMinutesWhenM() throws Exception {
        elapsed = new Elapsed("task", "msg", "start", "end", "M", labelGroup, false);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff start"), "pattern", 1000L, null, rawLineData, requestStartTimeMs, 1);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff end"), "pattern", 61000L, null, rawLineData, requestStartTimeMs, 2);

        Map<String, List<ElapsedInfo>> results = elapsed.getResults();
        ElapsedInfo elapsedTime = results.get("elapsed").get(0);
        assertThat(elapsedTime.getDuration(), is(1.0));

    }

    @Test
    public void shouldDisplayInHoursWhenH() throws Exception {
        elapsed = new Elapsed("task", "msg", "start", "end", "H", labelGroup, false);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff start"), "pattern", 1000L, null, rawLineData, requestStartTimeMs, 1);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff end"), "pattern", 120 * 60000L + 1000L, null, rawLineData, requestStartTimeMs, 2);

        Map<String, List<ElapsedInfo>> results = elapsed.getResults();
        ElapsedInfo elapsedTime = results.get("elapsed").get(0);
        assertThat(elapsedTime.getDuration(), is(2.0));

    }

    @Test
    public void shouldShowFraction() throws Exception {
        elapsed = new Elapsed("task", "msg", "start", "end", "H", labelGroup, false);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff start"), "pattern", 1000L, null, rawLineData, requestStartTimeMs, 1);
        elapsed.execute(fieldSet, fieldSet.getFields("stuff end"), "pattern", 150 * 60000L + 1000L, null, rawLineData, requestStartTimeMs, 2);

        Map<String, List<ElapsedInfo>> results = elapsed.getResults();
        ElapsedInfo elapsedTime = results.get("elapsed").get(0);
        assertThat(elapsedTime.getDuration(), is(2.5));

    }

    @Before
    public void setUp() throws Exception {
        elapsed = new Elapsed("task", "msg", "start", "end", "k", labelGroup, false);
        fieldSet = new FieldSet("^(\\w+) (.*)", "foo", "msg");


    }
}
