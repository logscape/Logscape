package com.liquidlabs.log.space;

import com.liquidlabs.common.collection.Arrays;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 25/02/2013
 * Time: 13:10
 * To change this template use File | Settings | File Templates.
 */
public class SearchSetTest  {
    Mockery context = new Mockery();


//    [
// {"col":9,"row":1,"size_x":2,"size_y":1,"id":"widget-1361552899714","type":"controller_widget","configuration":{"timeMode":"Standard","period":"900","fromTime":"2013-02-22T15:36:54.483Z","toTime":"2013-02-22T16:36:54.483Z"}},
// {"col":1,"row":1,"size_x":3,"size_y":2,"id":"widget-1361552899865","type":"chart_widget","configuration":{"title":"Unix Estate Table","widgetId":"#chart_widget-1361552043112","search":"type='Unx-VMStat' |  processes.avg(server,NbProcesses-) loadAvg1mi.avg(server,LoadAvg1Min%-) chart(table) buckets(1)"}},
// {"col":1,"row":3,"size_x":3,"size_y":2,"id":"widget-1361552899945","type":"chart_widget","configuration":{"title":"Do Stuff","widgetId":"#chart_widget-1361552108685","search":"* | _filename.count()"}},
// {"col":4,"row":1,"size_x":4,"size_y":3,"id":"widget-1361552900034","type":"chart_widget","configuration":{"title":"Sparky","widgetId":"#chart_widget-1361552279508","search":"* | _filename.count() chart(spark)"}}]


    @Test
    public void testShouldJson() {
        SearchSet ss = new SearchSet("test", Arrays.asList("SEARCH_ME"), "admin", 1, 2);
        final LogSpace logspace = context.mock(LogSpace.class);
        context.checking(new Expectations(){{

//            atLeast(1).of(db.resourceSpace).findResourceIdsBy("instanceId equals 0"); will(returnValue(new ArrayList<String>()));
            one(logspace).getSearch("SEARCH_ME", null); will(returnValue(new Search("name", "owner", Arrays.asList("*"), "", Arrays.asList(1), 90, "")));
        }});
        String json = ss.toJSON(logspace);
        System.out.println(json);
    }
    @Test
    public void testShouldJsonSet() {
        SearchSet ss = new SearchSet("test", Arrays.asList("SEARCH_ME","[","one","two","]","Last"), "admin", 1, 2);
        final LogSpace logspace = context.mock(LogSpace.class);
        context.checking(new Expectations(){{

//            atLeast(1).of(db.resourceSpace).findResourceIdsBy("instanceId equals 0"); will(returnValue(new ArrayList<String>()));
            one(logspace).getSearch("SEARCH_ME", null); will(returnValue(new Search("name", "owner", Arrays.asList("*"), "", Arrays.asList(1), 90, "")));
            one(logspace).getSearch("one", null); will(returnValue(new Search("name", "owner", Arrays.asList("111"), "", Arrays.asList(1), 90, "")));
            one(logspace).getSearch("two", null); will(returnValue(new Search("name", "owner", Arrays.asList("222"), "", Arrays.asList(1), 90, "")));
            one(logspace).getSearch("Last", null); will(returnValue(new Search("name", "owner", Arrays.asList("222"), "", Arrays.asList(1), 90, "")));
        }});
        String json = ss.toJSON(logspace);
        System.out.println(json);
    }

}
