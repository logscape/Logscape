package com.liquidlabs.common.regex;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class CsvParserTest {

    @Test
    public void shouldParseSimpleCsv() throws Exception {
        CsvParser csvParser = new CsvParser("123,abc,990");
        assertThat(csvParser.parse().size(), is(5));
    }

    @Test
    public void shouldParseCsvWithQuotesWithEmbeddedCommas() throws Exception {
        CsvParser csvParser = new CsvParser("123,\"foo,abc\",990");
        assertThat(csvParser.parse().size(), is(5));
        csvParser = new CsvParser("6.35E+17,\"500-9,999\", south east england, telecommunication");
        assertThat(csvParser.parse().size(), is(7));
        csvParser = new CsvParser("\"10,000+\",a few times");
        assertThat(csvParser.parse().size(), is(3));
    }

    @Test
    public void shouldReturnTheStringIfNoComma() throws Exception {
        CsvParser csvParser = new CsvParser("123");
        List<String> stringList = csvParser.parse();
        assertThat(stringList.size(), is(1));
        assertThat(stringList.get(0), is("123"));
    }

    @Test
    public void shouldNotHaveTrailingComma() throws Exception {
        CsvParser csvParser = new CsvParser("123,");
        List<String> stringList = csvParser.parse();
        assertThat(stringList.size(), is(2));
        assertThat(stringList.get(0), is("123"));
    }
}
