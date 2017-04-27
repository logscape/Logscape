package com.liquidlabs.log;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.log.space.Search;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrintUrlBuilderTest {

    private LogSpace logSpace;
    private PrintUrlBuilder printUrlBuilder;
    private Map<String, Object> params;

    @Before
    public void whenCreatingUrlsForPrinting() throws Exception {
        logSpace = mock(LogSpace.class);
        printUrlBuilder = new PrintUrlBuilder(logSpace);
        printUrlBuilder.withName("me");
        printUrlBuilder.withParam("client", "johno");
        printUrlBuilder.withParam("user", "damian");

    }

    @Test
    public void shouldBuildSomeUrlForWorkspace() {
        String expected = "http://" + NetworkUtils.getHostname() + ":8080/play/?Workspace=me&client=johno&user=damian";
        String url = printUrlBuilder.build();

        assertThat(url, is(expected));
    }

    @Test
    public void shouldBuildSomeUrlForSearch() {
        when(logSpace.getSearch("me", null)).thenReturn(new Search());
        String expected = "http://" + NetworkUtils.getHostname() + ":8080/play/?Search=me&client=johno&user=damian";
        String url = printUrlBuilder.build();
        assertThat(url, is(expected));
    }
}
