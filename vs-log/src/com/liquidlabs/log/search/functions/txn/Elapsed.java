package com.liquidlabs.log.search.functions.txn;

import com.carrotsearch.hppc.IntArrayList;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.functions.ValueSetter;
import org.joda.time.DateTime;

import java.util.*;

/**
 * Plots line chart o------------o with a Y axis showing the amount of time.... also maps onto real-time buckets so you have.
 * i.e.
 * |     o---------o
 * | o-----o      o----o  o----o
 * |---12:00---13:00---14:00--15:00
 * @author Neil
 *
 */
public class Elapsed implements SynthFunction {

    private final int totalBucketCount;

    public Elapsed(int totalBucketCount) {
        this.totalBucketCount = totalBucketCount;
    }
    Map<String, List<Long>> txnStartEndElapsed = new HashMap<String, List<Long>>();
    private List<Txn> txnsByD = new ArrayList<Txn>();
    private long fastest = Long.MAX_VALUE;
    private long slowest = 0;//Long.MAX_VALUE;
    private double divisor = 1.0;
    private String divisorUnit = "MS";
    private int lastPos = 999;

    public void apply(int currentPos, Map<String, IntArrayList> txns, ValueSetter valueSetter, long bucketStart, long bucketEnd, long requestStartTimeMs, Query query) {

        boolean isTableFormat = query != null ? query.sourceQuery().contains("chart(table)") : false;

//        System.out.println("==" + currentPos + "========================:isNull:" + (txns == null));

        // rebuild the txns so we only do it once
        if (currentPos == 0 || currentPos < lastPos) {
            txnStartEndElapsed.clear();

            // build all of the txns
            Set<String> keySet = txns.keySet();
            txnsByD = new ArrayList<Txn>();
            for (String txnId : keySet) {
                List<Long> startEndElapsed = getStartEndElapsed(txnId, txns.get(txnId), requestStartTimeMs);
                txnStartEndElapsed.put(txnId, startEndElapsed);
                txnsByD.add(new Txn(txnId,startEndElapsed.get(2), startEndElapsed.get(0)));
                slowest = Math.max(slowest, startEndElapsed.get(2));
                fastest = Math.min(fastest, startEndElapsed.get(2));
            }
            // sort from slowest to fastest
            Collections.sort(txnsByD, new Comparator<Txn>(){

                public int compare(Txn o1, Txn o2) {
                    return Long.valueOf(o2.duration).compareTo(o1.duration);
                }
            });
            if (slowest < 10000) divisorUnit = "MS";
            else if (slowest < DateUtil.MINUTE) divisorUnit = "S";
            else if (slowest < DateUtil.MINUTE * 60) divisorUnit = "M";
            else divisorUnit = "H";

            setDivisor(divisorUnit);

        }
        lastPos = currentPos;
        //System.out.println("ElapsedTime: "  + new DateTime(bucketStart) + " -> " + new DateTime(bucketEnd));
        for (Txn txn : txnsByD) {
            String txnId = txn.id;
            List<Long> startEndElapsed = txnStartEndElapsed.get(txn.id);
//            System.out.println("> Start: "  + new DateTime(startEndElapsed.get(0)));

            // handle this bucket item
            // startTime
            double value = StringUtil.roundDouble(startEndElapsed.get(2) / divisor, 2);

            if (isInBucket(bucketStart,bucketEnd, startEndElapsed.get(0))) {
//				System.out.println(">" + txnId);
                if (isTableFormat) valueSetter.set(txnId + "_" +  divisorUnit, value, true);
                else valueSetter.set(txnId + "_" + value + divisorUnit, value, true);
            }
            // endTime
            else if (isInBucket(bucketStart,bucketEnd, startEndElapsed.get(1))) {
//				System.out.println("<" + txnId);
                if (isTableFormat) valueSetter.set(txnId + "_" +  divisorUnit, value, true);
                else valueSetter.set(txnId + "_" + value + divisorUnit, value, true);
            }
            // When - end and start we both in the previous bucket...then drop one in here
            if (isStartAndEndInPreviousBucket(bucketStart, bucketEnd, startEndElapsed)) {
//				System.out.println("=" + txnId);
                if (isTableFormat) valueSetter.set(txnId + "_" +  divisorUnit, value, true);
                else valueSetter.set(txnId + "_" + value + divisorUnit, value, true);
            }
        }

        if (currentPos == totalBucketCount -1) {
            txnStartEndElapsed.clear();
        }

    }
    private static class Txn {
        private final Long from;
        public Txn(String txnId, Long duration, Long from) {
            id = txnId;
            this.duration = duration;
            this.from = from;
        }
        String id;
        long duration;
        public String toString() {
            return this.id + " ms:" + this.duration + " start:" + DateUtil.shortDateTimeFormat7.print(this.from);
        }
    }

    private boolean isStartAndEndInPreviousBucket(long bucketStart, long bucketEnd, List<Long> startEndElapsed) {
        long bucketW = bucketEnd - bucketStart;
        long previousBStart = bucketStart - bucketW;
        long previousBEnd = bucketStart -1;
        return (isInBucket(previousBStart, previousBEnd, startEndElapsed.get(0)) && isInBucket(0, previousBEnd, startEndElapsed.get(1)));
    }

    private boolean isInBucket(long bucketStart, long bucketEnd, Long time) {
        long bucketW = 1;//(bucketEnd - bucketStart) / 4;
        return (time + bucketW >= bucketStart && time + bucketW <= bucketEnd );
    }

    private List<Long> getStartEndElapsed(String txnId, IntArrayList list, long requestStartTimeMs) {
        if (txnStartEndElapsed.containsKey(txnId)) return txnStartEndElapsed.get(txnId);
        long startTime = requestStartTimeMs + ((long)list.get(0)) * SyntheticTransAccumulate.DIVISOR;
        long endTime = requestStartTimeMs + ((long)list.get(list.size()-1)) * SyntheticTransAccumulate.DIVISOR;
        return Arrays.asList(startTime, endTime, (endTime - startTime));
    }


    private void setDivisor(String displayUnit) {
        if ("S".equalsIgnoreCase(displayUnit)) {
            this.divisor = 1000.0;
        } else if ("M".equalsIgnoreCase(displayUnit)) {
            this.divisor = 60000.0;
        } else if ("H".equalsIgnoreCase(displayUnit)) {
            this.divisor = 60.0 * 60000.0;
        }
    }

}
