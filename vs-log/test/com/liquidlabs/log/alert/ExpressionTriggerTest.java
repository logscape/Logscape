package com.liquidlabs.log.alert;

import com.liquidlabs.log.LogRequestBuilder;
import com.liquidlabs.log.NullLogSpace;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.log.space.agg.ClientHistoItem;
import com.liquidlabs.log.space.agg.ClientHistoItem.SeriesValue;
import com.liquidlabs.log.space.agg.HistogramHandler;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExpressionTriggerTest {


    @Test
    public void shouldEvaluatePostAggProperly() throws Exception {
        String test = "CPU | CPU.avg(,AVG) AVG.gt(10)";

        // post aggs are applied to the histogram and before the
        // so CPU.avg(,AVG) AVG.gt(10)
//        List<Map<String, Bucket>> histogram = null;
//        LogRequest request = new LogRequestBuilder().getLogRequest("test", Arrays.asList("CPU | CPU.avg(,AVG) AVG.gt(10)"),"", 0, System.currentTimeMillis());
//        NullLogSpace logSpace = new NullLogSpace();
//        List<ClientHistoItem> histogramForClient = new HistogramHandler(Executors.newScheduledThreadPool(1), request, logSpace).getHistogramForClient(histogram, request);

        ExpressionTrigger.ExpressionEvaluator eval = new ExpressionTrigger.ExpressionEvaluator("AVG.gt(10)");

        ClientHistoItem h1 = new ClientHistoItem();
        List<ClientHistoItem> histo = new ArrayList<ClientHistoItem>();
        histo.add(h1);

        SeriesValue svTrue = new ClientHistoItem.SeriesValue();
        h1.series.put("CPU", svTrue);
        svTrue.fieldName = "CPU";
        svTrue.value = 11;

        assertTrue("Should be True", eval.isTriggered(histo));
        svTrue.value = 10;
        assertFalse("Should be False", eval.isTriggered(histo));
    }



    @Test
	public void shouldEvaluateFilterProperly() throws Exception {
		ExpressionTrigger.ExpressionEvaluator eval = new ExpressionTrigger.ExpressionEvaluator("CPU.gt(10)");

        ClientHistoItem h1 = new ClientHistoItem();
        List<ClientHistoItem> histo = new ArrayList<ClientHistoItem>();
        histo.add(h1);

        SeriesValue svTrue = new ClientHistoItem.SeriesValue();
        h1.series.put("CPU", svTrue);
		svTrue.fieldName = "CPU";
		svTrue.value = 11;

		assertTrue("Should be True", eval.isTriggered(histo));
		svTrue.value = 10;
		assertFalse("Should be False", eval.isTriggered(histo));
	}


}
