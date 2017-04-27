package com.liquidlabs.vso.container;

import com.liquidlabs.vso.work.InvokableUI;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public class DumbConsumer implements Consumer {
    private static final Logger LOGGER = Logger.getLogger(DumbConsumer.class);

    List<String> myResources = new ArrayList<String>();
    private Double queueLength = 10.0;

    private Map<String, String> propertyMap;
    private CountDownLatch adds;
    private CountDownLatch removes;

    public DumbConsumer(CountDownLatch adds, CountDownLatch removes) {
        this.adds = adds;
        this.removes = removes;
    }

    static class DumbMetric implements Metric {

        private final String name;
        private final Double value;

        public DumbMetric(String name, Double value) {
            this.name = name;
            this.value = value;

        }

        public String name() {
            return name;
        }

        public Double value() {
            return value;
        }
    }

    public DumbConsumer() {
        this(new CountDownLatch(1), new CountDownLatch(1));
    }


    public Metric[] collectMetrics() {
        DumbMetric dumbMetric = new DumbMetric("queueLength", queueLength);
        LOGGER.info(">>>>>>>>>>>>>>> Dumb collectMetrics, QLength:" + queueLength);
        return new Metric[]{dumbMetric};
    }

    public String name() {
        return "DumbArseConsumer";
    }

    public void setName(String name) {
    }

    public Set<String> collectResourceIdsForSync() {
        return new HashSet<String>(this.myResources);
    }

    public String release() {
        if (!myResources.isEmpty()) {
            return myResources.remove(0);
        }
        return "";
    }

    public void add(String requestId, List<String> resourceIds, AddListener addListener) {
        LOGGER.info(">>>>>>>>>>>>>>> Dumb add:" + resourceIds);
        myResources.addAll(resourceIds);
        for (String resourceId : resourceIds) {
            addListener.success(resourceId);
        }
        adds.countDown();
    }

    public int getUsedResourceCount() {
        return myResources.size();
    }

    public void setQueueLength(Double queueLength) {
        LOGGER.info(">>>>>>>>>>>>>>> SetQueueLength:" + queueLength);
        this.queueLength = queueLength;
    }

    public double getQueueLength() {
        return queueLength;
    }

    public void take(String requestId, List<String> resourceIds) {
        LOGGER.info(">>>>>>>>>>>>>>> take:" + resourceIds);
        if (myResources.isEmpty()) return;
        myResources.removeAll(resourceIds);
        for(String foo:resourceIds) {
            removes.countDown();
        }
    }

    public List<String> release(String requestId, List<String> givenResourceIds, int requiredCount) {
        List<String> result = new ArrayList<String>();
        Iterator<String> iterator = myResources.iterator();
        while (iterator.hasNext()) {
            String myResourceId = (String) iterator.next();
            if (result.size() < requiredCount) {
                for (String resourceId : givenResourceIds) {
                    if (myResourceId.equals(resourceId)) {
                        result.add(myResourceId);
                        LOGGER.info(">>>>>>>>>>>>>>> Allowing Release:" + result);
                    }
                }
            }
        }
        this.releasedResources.addAll(result);
        for (String foo : result) {
            removes.countDown();
        }
        return result;
    }

    CopyOnWriteArrayList<String> releasedResources = new CopyOnWriteArrayList<String>();

    public List<String> getReleasedResources() {
        ArrayList<String> result = new ArrayList<String>(releasedResources);
        releasedResources.clear();
        return result;
    }

    /**
     * Called from Remove event
     */
    public List<String> getResourceIdsToRelease(String template, Integer resourcesToFree) {
        List<String> result = new ArrayList<String>();
        for (String string : myResources) {
            if (result.size() < resourcesToFree) {
                result.add(string);
            }
        }
        myResources.removeAll(result);
        releasedResources.addAll(result);
        for (String foo : result) {
            removes.countDown();
        }
        return result;
    }

    public void setVariables(Map<String, String> propertyMap) {
        this.propertyMap = propertyMap;
    }

    public InvokableUI getUI() {
        return null;
    }

    public void setInfo(String consumerId, String workIntent, String fullBundleName) {
    }

    public void synchronizeResources(Set<String> set) {
    }

    public int getRunInterval() {
        return 30;
    }
}
