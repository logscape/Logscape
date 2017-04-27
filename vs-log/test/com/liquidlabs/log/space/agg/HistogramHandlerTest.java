package com.liquidlabs.log.space.agg;

import com.liquidlabs.log.NullLogSpace;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.functions.Count;
import com.liquidlabs.log.space.LogRequest;
import junit.framework.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Executors;

/**
 * Created by neil.avery on 16/02/2016.
 */
public class HistogramHandlerTest {

    @Test
    public void testEvictTopItems() throws Exception {

        Init init = new Init().invoke();
        HistogramHandler handler = init.getHandler();
        LogRequest request = init.getRequest();
        List<ClientHistoItem> clientHisto = init.getClientHisto();
        Map<Integer, Set<String>> functionTags = init.getFunctionTags();
        int qpos = init.getQpos();


        List<List<String>> eventOrderByGroup = new ArrayList<>();
        eventOrderByGroup.add(Arrays.asList("A1","B1","C1"));

        // IMPORTANT - force C1 to be evicted
        request.query(0).setTopLimit((short) 2);


        // TEST
        handler.evictTopOrBottomItems(request, clientHisto, eventOrderByGroup, functionTags, qpos);


        // Check size
        Assert.assertEquals(2, clientHisto.get(0).series.size());

        // Should have evicted "C1" - the last in the sorted list ;)
        Assert.assertTrue(!clientHisto.get(0).series.toString().contains("C1"));
    }

    @Test
    public void testEvictBottomItems() throws Exception {

        Init init = new Init().invoke();
        HistogramHandler handler = init.getHandler();
        LogRequest request = init.getRequest();
        List<ClientHistoItem> clientHisto = init.getClientHisto();
        Map<Integer, Set<String>> functionTags = init.getFunctionTags();
        int qpos = init.getQpos();


        List<List<String>> eventOrderByGroup = new ArrayList<>();
        eventOrderByGroup.add(Arrays.asList("C1","B1","A1"));

        // IMPORTANT - force C1 to be evicted
        request.query(0).setTopLimit((short) -2);


        // TEST
        handler.evictTopOrBottomItems(request, clientHisto, eventOrderByGroup, functionTags, qpos);


        // Check size
        Assert.assertEquals(2, clientHisto.get(0).series.size());

        // Should have evicted "C1" - the last in the sorted list ;)
        Assert.assertTrue(!clientHisto.get(0).series.toString().contains("A1"));

    }



    private class Init {
        private HistogramHandler handler;
        private LogRequest request;
        private List<ClientHistoItem> clientHisto;
        private Map<Integer, Set<String>> functionTags;
        private int qpos;
        private Query query;

        public HistogramHandler getHandler() {
            return handler;
        }

        public LogRequest getRequest() {
            return request;
        }

        public List<ClientHistoItem> getClientHisto() {
            return clientHisto;
        }

        public Map<Integer, Set<String>> getFunctionTags() {
            return functionTags;
        }

        public int getQpos() {
            return qpos;
        }

        public Query getQuery() {
            return query;
        }

        public Init invoke() {
            handler = new HistogramHandler(Executors.newScheduledThreadPool(1), new LogRequest(), new NullLogSpace());
            request = new LogRequest();
            clientHisto = new ArrayList();
            ClientHistoItem firstItem = new ClientHistoItem();
            firstItem.set("A1", 300, 0, 0, 0);
            firstItem.set("B1", 200, 0, 0, 0);
            firstItem.set("C1", 100, 0, 0, 0);
            clientHisto.add(firstItem);

            // verify setup
            Assert.assertEquals(3, clientHisto.get(0).series.size());


            functionTags = new HashMap<>();
            qpos = 0;
            functionTags.put(qpos, new HashSet<String>(Arrays.asList("")));

            query = new Query(0, "(*)");
            query.addFunction(new Count("", "", "_filename"));
            request.queries().add(query);
            return this;
        }
    }
}