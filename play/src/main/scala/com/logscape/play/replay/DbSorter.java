package com.logscape.play.replay;

import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.search.TimeUID;
import org.mapdb.Fun;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 02/05/2013
 * Time: 12:33
 * To change this template use File | Settings | File Templates.
 */
public class DbSorter {

    static List<TimeUID> filterToTime(List<TimeUID> rr, long from, long to) {
        ArrayList<TimeUID> results = new ArrayList<TimeUID>();
        for (TimeUID aLong : rr) {
            if (aLong.timeMs >= from && aLong.timeMs <= to) results.add(aLong);
        }
        return results;
    }
    static public Iterator<TimeUID> getIterator(final  Map<TimeUID, ReplayEvent> db, final int sortCol, final String sortFieldName, final boolean ascending, final FieldSet fieldSet, final ReplayEvent.Mode mode, final long fromMs, final long toMs) {
        if (sortCol == 0){
          if (ascending) {
              List<TimeUID> results = new ArrayList<TimeUID>(db.keySet());
              return filterToTime(results, fromMs, toMs).iterator();
          }
            else {
              List<TimeUID> results = new ArrayList<TimeUID>(db.keySet());
              Collections.reverse(results);

              return filterToTime(results, fromMs, toMs).iterator();

          }
        }
        List<TimeUID> results = new ArrayList<TimeUID>(db.keySet());
        if (sortCol == 0 || results.size() == 0) return filterToTime(results, fromMs, toMs).iterator();

        ReplayEvent first = db.values().iterator().next();

        fieldSet.getFieldValues(first.getRawData(), -1, -1, -1);
        List<String> fieldNames = fieldSet.getFieldNames(false, true, true, false, true);



        Collections.sort(results, new Comparator<TimeUID>(){

            @Override
            public int compare(TimeUID t1, TimeUID t2) {

                ReplayEvent r1 = db.get(t1);
                ReplayEvent r2 = db.get(t2);
                if (r1 == null || r2 == null) {
                    //System.out.println("EventsDB.DBSorter GOT NULL EventDB thing");
                    return t1.compareTo(t2);
                }
                try {
                    if (mode.equals(ReplayEvent.Mode.raw) || mode.equals(ReplayEvent.Mode.structured)) {

                        if (sortCol == 0) {
                            // time
                            return ascending ?  t1.compareTo(t2) :  t2.compareTo(t1);

                        } else if (sortCol == 1) {
                            // msg
                            return ascending ?  r1.getRawData().compareTo(r2.getRawData()) :  r2.getRawData().compareTo(r1.getRawData());

                        } else if (sortCol == 2) {
                            // host

                            return ascending ?  r1.getHostname().compareTo(r2.getHostname()) :  r2.getHostname().compareTo(r1.getHostname());
                        } else if (sortCol == 3) {
                            // tagt
                            return ascending ?  r1.getDefaultField(FieldSet.DEF_FIELDS._tag).compareTo(r2.getDefaultField(FieldSet.DEF_FIELDS._tag)) :
                                    r2.getDefaultField(FieldSet.DEF_FIELDS._tag).compareTo(r1.getDefaultField(FieldSet.DEF_FIELDS._tag));

                        } else if (sortCol == 4) {
                            // type
                            return ascending ?  r1.getDefaultField(FieldSet.DEF_FIELDS._type).compareTo(r2.getDefaultField(FieldSet.DEF_FIELDS._type)) :
                                    r2.getDefaultField(FieldSet.DEF_FIELDS._type).compareTo(r1.getDefaultField(FieldSet.DEF_FIELDS._type));

                        }
                    } else {
                        String v1 = getFieldValue(r1, fieldSet, sortFieldName);
                        String v2 = getFieldValue(r2, fieldSet, sortFieldName);

                        return ascending ? v1.compareTo(v2) : v2.compareTo(v1);

                    }
                    return ascending ?  r1.getHostname().compareTo(r2.getHostname()) :  r2.getHostname().compareTo(r1.getHostname());
                } catch (Throwable t) {
                    t.printStackTrace();
                    return t1.compareTo(t2);
            }
            }
        });

        return filterToTime(results, fromMs, toMs).iterator();
    }
    private static String getFieldValue(ReplayEvent event, FieldSet fieldSet, String fieldName) {
        try {
            String[] events = fieldSet.getNormalFields(event.getRawData());
            // need to look at existing fields - then if needed pull in a disco-field
            String fieldValue = fieldSet.getFieldValue(fieldName, events);
            if (fieldValue != null) return  fieldValue;

            // now fallback on the discovered fields
            String[] e1 = fieldSet.getFields(event.getRawData(), -1, -1, event.getTime());
            if (event.fieldSetId().equals(fieldSet.getId())) {
                fieldValue = fieldSet.getFieldValue(fieldName, e1);
                if (fieldValue == null) return "";
                return fieldValue;
            } else {
                return "";
            }



//                    String[] e1 = fieldSet.getFields(event.getRawData(), -1, -1, event.getTime());
//            if (fieldName.startsWith("_")) return event.getDefaultField(fieldName);
//            if (event.fieldSetId().equals(fieldSet.getId())) {
//                String fieldValue = fieldSet.getFieldValue(fieldName, e1);
//                if (fieldValue == null) return "";
//                return fieldValue;
//            } else {
//                return "";
//            }
//            return fieldValue;
        } catch (Throwable t) {
            return "";
        }
    }

}
