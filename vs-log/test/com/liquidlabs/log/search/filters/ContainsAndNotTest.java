package com.liquidlabs.log.search.filters;

import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ContainsAndNotTest {
	
	FieldSet fieldSet = FieldSets.get();
	private String lineData;
    private int lineNumber;



    @Test
    public void shouldNotMatchMultiple() throws Exception {
        String lineData = "85.119.21.2 - - [04/Feb/2014:05:50:19 -0800] \"GET /images/track.png?version=Logscape-2.1-31-Jan-14-0956 HTTP/1.1\" 304 - \"http://cdcs192506:8080/play/login\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.102 Safari/537.36\"";
        Not filter = new Not("stuff", "data", Arrays.asList("217.20.25.200","85.119.21.2"));

        assertFalse(filter.isPassed(fieldSet, new String[] { "217.20.25.200" }, "nothing line", null, 1));
        fieldSet = FieldSets.get();
        assertFalse(filter.isPassed(fieldSet, new String[] { "85.119.21.2" }, "nothing line", null, 2));
        fieldSet = FieldSets.get();
        assertTrue(filter.isPassed(fieldSet, new String[] { "99.119.21.2" }, "nothing line", null, 3));
    }



    @Test
    public void shouldNotMatch() throws Exception {
        String lineData = "85.119.21.2 - - [04/Feb/2014:05:50:19 -0800] \"GET /images/track.png?version=Logscape-2.1-31-Jan-14-0956 HTTP/1.1\" 304 - \"http://cdcs192506:8080/play/login\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.102 Safari/537.36\"";
        Not filter = new Not("stuff", "data", "NOT_ME");
        assertFalse(filter.isPassed(fieldSet, new String[] { "NOT_ME" }, "nothing line", null, lineNumber));
    }


    @Test
	public void shouldMatchEscapedBrackets() throws Exception {
		Contains filter = new Contains("stuff", "", "load\\(\\)");
		assertFalse(filter.isPassed(fieldSet, new String[] { "" }, "this is a load() line", null, lineNumber));
	}
	
	@Test
	public void shouldNotMatchEmptyString() throws Exception {
		Contains filter = new Contains("stuff", FieldSets.fieldName, Arrays.asList("A","B"));
		assertFalse(filter.isPassed(fieldSet, new String[] { "" }, lineData, null, lineNumber));
		
	}
	@Test
	public void shouldNotMatchNull() throws Exception {
		Contains filter = new Contains("stuff", FieldSets.fieldName, Arrays.asList("A","B"));
		assertFalse(filter.isPassed(fieldSet, new String[] { null }, lineData, null, lineNumber));
		
	}
	@Test
	public void shouldHandleGroupToopSmall() throws Exception {
		Contains filter = new Contains("stuff", FieldSets.fieldName, Arrays.asList("A","B"));
		assertTrue(filter.isPassed(fieldSet, new String[] { "A" }, lineData, null, lineNumber));
	}

	@Test
	public void testShouldHandleMultiple() throws Exception {
		Contains filter = new Contains("stuff", FieldSets.fieldName, Arrays.asList("A","B"));
		assertFalse(filter.isPassed(fieldSet, new String[] { "line" }, lineData, null, lineNumber));
        fieldSet.reset();
		assertTrue(filter.isPassed(fieldSet, new String[] {"Aaaa" }, lineData, null, lineNumber));
        fieldSet.reset();
		assertTrue(filter.isPassed(fieldSet, new String[] {"Bbbb" }, lineData, null, lineNumber));
	}

    @Test
    public void shouldBeCaseInsensitive() {
        final Contains contains = new Contains("foo", FieldSets.fieldName, Arrays.asList("NETWORK"));
        assertThat(contains.isPassed(fieldSet, new String[]{"Network:en0 /10.0.1.65"}, lineData, null, lineNumber), is(true));
    }
}
