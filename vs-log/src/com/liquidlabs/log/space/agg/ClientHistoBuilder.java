package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.LogProperties;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Builds Table XML
 */
public class ClientHistoBuilder {
    private final static Logger LOGGER = Logger.getLogger(ClientHistoBuilder.class);


    public String buildTableXml(ClientHistoItem item, String subscriber, boolean groupBy1Dimension, String groupFieldName, String topFieldName, int topLimit, List<String> seriesList, List<String> topSortedFields, ScheduledExecutorService scheduler, boolean isTop, boolean isTable) {

        if (item.meta == null) item.meta = new Meta();
//		sortSeriesValuesMap(topSortedSeriesList);

        int keyLength = 1;
        String firstKey = seriesList.isEmpty() ? "" : seriesList.get(0);
        String longestKey = "";
        for (String string : seriesList) {
            if (string.contains(".value.")) continue;
            String[] split = string.split(LogProperties.getFunctionSplit());
            if (split.length > keyLength) {
                keyLength = split.length;
                longestKey = string;
            }
        }
        if (keyLength > 3) {
            keyLength = 3;
        }
        if (LOGGER.isDebugEnabled()) LOGGER.debug("KeyLength to:" + keyLength + " LongestKey:" + longestKey);

        String tag = firstKey.split(LogProperties.getFunctionSplit())[0];
        NumberFormat numberFormat = NumberFormat.getInstance();
        if (isSingleGroupable(seriesList)) {
            groupBy1Dimension = true;
        }
        if (groupBy1Dimension && keyLength != 3  && keyLength != 2) {
            item.meta.xml = convertToGroups(item.series, groupFieldName, topFieldName, topLimit, seriesList, isTop);
        } else if (keyLength == 1) {
            // label = Stuff, Value = 100
            // <item>
            //    <level>Stuff</leven>
            //    <value>100</value>
            // </item>
            // <item>
            //   <level>Stuff</level>
            //    <value>200</value>
            sortSeriesValuesMap(item, topSortedFields);

            StringBuilder xml = new StringBuilder("<xml>\n");
            String columnName = item.series.size() > 0 ? item.series.values().iterator().next().func : "---";
            xml.append(getOneElementItemsAsXML(item, groupFieldName, numberFormat,columnName));
            xml.append("</xml>");
            item.meta.xml = xml.toString();


        } else if (keyLength == 2) {
            // level.count() => level_DEBUG
            // <item>
            //    <level>DEBUG</leven>
            //    <value>100</value>
            // </item>
            // <item>
            //   <level>INFO</level>
            //    <value>100</value>
//			sortSeriesValuesMap(seriesList);
            if (!isTable) sortSeriesValuesMapAccordingly(item, seriesList,topSortedFields, isTop);

            item.meta.xml = getTwoElementItemsAsXML(item, subscriber, tag, groupFieldName, scheduler, topFieldName, isTop, topSortedFields);

            // etc.
        } else if (keyLength == 3) {

            // we may have an XYsetup
            // i.e.
            //    total_DistributedCache_2, total_DistributedCache_3, ha_DistributedCache_NODE-SAFE,senior_DistributedCache_10]
            // OR
            //    level.count(filename) => level_agent.log_INFO level_boot.log_WARN

            //
            // <item>
            //    <filename>agent.log</filename>
            //    <DEBUG>100</DEBUG>
            //    <INFO>100</INFO>
            // </item>
            // <item>
            //   <level>INFO</level>
            //    <value>100</value>
//			evictItemsForTable(evictionList);
            sortSeriesValuesMapAccordingly(item, seriesList,topSortedFields, isTop);
            mapWith3Fields(item, numberFormat, topLimit);
        }
        if(item.series != null && item.series.size() > 1) {
            // need to keep allSeriesNames
//            this.meta.allSeriesNames.clear();
            String key = item.series.keySet().iterator().next();
            ClientHistoItem.SeriesValue value = item.series.get(key);
            item.series.clear();
            item.series.put(key, value);
        }
        item.meta.xml = StringUtil.escapeXMLEntities(item.meta.xml);


        return item.meta.xml;
    }
    /**
     * See if we can extract common groups from the series values.
     * i.e. series name resType_PNG, resType_jpg bytes_PNG, bytes_jpg
     * @param seriesList
     * @return
     */
    boolean isSingleGroupable(List<String> seriesList) {
        Map<String, List<String>> seriesGroups = new HashMap<String, List<String>>();
        for (String seriesName : seriesList) {
            int lastDash = seriesName.lastIndexOf(LogProperties.getFunctionSplit());

            if (lastDash > -1) {
                // PNG etc
                String itemName = seriesName.substring(lastDash);
                if (!seriesGroups.containsKey(itemName)) seriesGroups.put(itemName, new ArrayList<String>());
                seriesGroups.get(itemName).add(itemName);
                if (seriesGroups.get(itemName).size() > 1) return true;
            } else {
                return false;
            }
        }
        return false;
    }
    String getOneElementItemsAsXML(ClientHistoItem item, String tag, NumberFormat numberFormat, String columnName) {
        if (tag == null) tag = "";
        StringBuilder xml = new StringBuilder();
        List<ClientHistoItem.SeriesValue> values = new ArrayList<ClientHistoItem.SeriesValue>(item.series.values());
        Collections.sort(values, new Comparator<ClientHistoItem.SeriesValue>() {
            public int compare(ClientHistoItem.SeriesValue o1, ClientHistoItem.SeriesValue o2) {
                return Double.valueOf(o2.value).compareTo(o1.value);
            }
        });
        if (values.size() > 0 && values.iterator().next().label.contains(".value.")) {
            for (ClientHistoItem.SeriesValue sv : values) {
                xml.append("<item>");
                String[] keyValue = sv.label.split(".value.");
                String key = keyValue[0];
                if (key.contains( LogProperties.getFunctionSplit())) {
                    key = key.split( LogProperties.getFunctionSplit())[0];
                }
                xml.append("\t<Key>").append(key).append("</Key>");
                xml.append("\t\t<Value>").append(keyValue[1]).append("</Value>");
                xml.append("\t\t<Value_view>").append(sv._view).append("</Value_view>");
                xml.append("</item>\n");
            }
            item.meta.allSeriesNames = new ArrayList(Arrays.asList(new String[] { "Key", "Value" } ));


        }  else {
            if (tag.contains("/")) tag = tag.replaceAll("/", "-");
            for (ClientHistoItem.SeriesValue sv : values) {
                xml.append("<item>");
                xml.append(String.format("\t<%s>", tag)).append(sv.label.replace(tag+LogProperties.getFunctionSplit(), "")).append(String.format("</%s>", tag));
                xml.append(String.format("\t\t<%s>", columnName)).append(numberFormat.format(sv.value)).append(String.format("</%s>", columnName));
                xml.append(String.format("\t\t<%s_view>", columnName)).append(sv._view).append(String.format("</%s_view>", columnName));
                xml.append("</item>\n");
            }
            item.meta.allSeriesNames = new ArrayList(Arrays.asList( tag, columnName));
        }
        return xml.toString();
    }
    // <item>
    //    <filename>agent.log</filename>
    //    <package>100</DEBUG>
    //    <lastLevel>INFO</lastLevel>
    // </item>

    String getTwoElementItemsAsXML(ClientHistoItem item, String subscriber, String tag, String groupByFieldname, ScheduledExecutorService scheduler, String topFieldname, boolean isTop, List<String> topSortedFields) {

        LinkedHashMap<String, List<String[]>> rowData = new LinkedHashMap<String,List<String[]>>();

        int topColumn  = -1;
//		boolean gotCol = false;
//
//		// need to sort the groupId so we get them all in the same order
//		List<SeriesValue> values = new ArrayList<SeriesValue>(this.series.values());
//		Collections.sort(values, new Comparator<SeriesValue>(){
//			public int compare(SeriesValue o1, SeriesValue o2) {
//				return Integer.valueOf(o1.groupId).compareTo(o2.groupId);
//			}
//		});
//
        for (String fieldName : topSortedFields) {
            ClientHistoItem.SeriesValue sv = item.series.get(fieldName);
            String[] rowColValueView = null;
            if (sv.label.indexOf(".by.") > -1) {
                String[] addByRow = addByRow(sv);
                if (addByRow != null) rowColValueView = addByRow;
                else continue;
            } else {
                String[] split = sv.label.split(LogProperties.getFunctionSplit());
                if (split.length != 2) {
                    split = new String[] { sv.fieldName, split[0] };
                }
                rowColValueView = new String[] { split[1], split[0], getUIFriendlyDecimal(sv), sv._view, sv.fieldName, sv.func, sv.groupId+"" };
            }

            if (!rowData.containsKey(rowColValueView[0])) rowData.put(rowColValueView[0], new ArrayList<String[]>());
            rowData.get(rowColValueView[0]).add(rowColValueView);
        }
        // sort the columns orders properly - by groupId - which is the statement position
        for (List<String[]> rowInfo : rowData.values()) {
            Collections.sort(rowInfo, new Comparator<String[]>(){
                public int compare(String[] o1, String[] o2) {
                    return Integer.valueOf(o1[6]).compareTo(Integer.valueOf(o2[6]));
                }
            });
        }

        TablePager tablePager = new TablePager(subscriber, groupByFieldname, 10 * 1024, tag, rowData, false);
        if (topFieldname.length() > 0){
            topColumn = tablePager.getColumnIndex(topFieldname);
        }
        if (scheduler != null) TablePager.write(scheduler, tablePager);
        String page = tablePager.getPage(1, topColumn, !isTop);
        item.meta.allSeriesNames = tablePager.tableColumnnNames;
        return page ;
    }
    private int getFunctionCount(Collection<ClientHistoItem.SeriesValue> values) {
        Set<String> functions = new HashSet<String>();
        for (ClientHistoItem.SeriesValue seriesValue : values) {
            functions.add(seriesValue.fieldName + seriesValue.func);
        }
        return functions.size();
    }
    /**
     * Need to go more than the top item count when multiple columns... so use the ordering of the top list then fill in the remainder (i.e. dont truncate here)
     * @param allSeriesList
     * @param topSortedFields
     * @param isTop
     */
    private void sortSeriesValuesMapAccordingly(ClientHistoItem item, List<String> allSeriesList, List<String> topSortedFields, boolean isTop) {
        List<String> sortByMe = getFunctionCount(item.series.values()) == 1 ? topSortedFields : allSeriesList;

        if (!isTop) Collections.reverse(sortByMe);

        // sort to a LinkedHashMap
        List<String> seriesList = new ArrayList<String>();
        Map<String, ClientHistoItem.SeriesValue> sortedSeries = new LinkedHashMap<String, ClientHistoItem.SeriesValue>();
        // sort the top items
        for (String seriesItem : sortByMe) {
            seriesList.remove(seriesItem);
            ClientHistoItem.SeriesValue sv = item.series.get(seriesItem);
            if (sv != null) sortedSeries.put(seriesItem, sv);
        }
        for (String seriesItem : seriesList) {
            ClientHistoItem.SeriesValue sv = item.series.get(seriesItem);
            if (sv != null) sortedSeries.put(seriesItem, sv);
        }

        item.series = sortedSeries;
    }
    private void sortSeriesValuesMap(ClientHistoItem item, List<String> topSortedSeriesList) {
        // sort to a LinkedHashMap
        Map<String, ClientHistoItem.SeriesValue> sortedSeries = new LinkedHashMap<String, ClientHistoItem.SeriesValue>();
        for (String topSeriesItem : topSortedSeriesList) {
            ClientHistoItem.SeriesValue sv = item.series.get(topSeriesItem);
            if (sv != null) sortedSeries.put(topSeriesItem, sv);
        }
        item.series = sortedSeries;
    }
    //					total_3.by.DistributedCache ha_NODE_SAFE.by.DistributedCache
    private String[] addByRow(ClientHistoItem.SeriesValue sv) {
        int index = sv.label.indexOf(".by.");
        String[] parts = new String[] {  sv.label.substring(0, index), sv.label.substring(index + 4) };
        if (parts.length!=2) {
            return new String[0];
        }
        String[] col_val = StringUtil.splitFast(parts[0], LogProperties.getFunctionSplit().charAt(0), 2, false);
        if (col_val.length != 2) {
            return new String[] { parts[1], sv.fieldName, col_val[0], sv._view, sv.fieldName, sv.func, sv.groupId+"" };
        }
        String column=col_val[0];
        if (column.equals("by")) column = sv.fieldName;
        String value=col_val[1];
        String itemRow = parts[1];

        return new String[] { itemRow, column, value, sv._view, sv.fieldName, sv.func, sv.groupId+"" };
    }
    private void mapWith3Fields(ClientHistoItem item, NumberFormat numberFormat, int topLimit) {
        final Map<String, List<String>> mapToAgg = new LinkedHashMap<String, List<String>>();
        Collection<ClientHistoItem.SeriesValue> values = item.series.values();
        List<String> columns = new ArrayList<String>();
        columns.add("X");
        for (ClientHistoItem.SeriesValue sv : values) {
            try {
                String[] parts = sv.label.split(LogProperties.getFunctionSplit());
                if (parts.length==2) {
                    System.err.println(getClass().getCanonicalName() + " PROBLEM WITH:" + sv.label + " parts:" + parts.length);
                    continue;
                }
                String colName = StringUtil.fixForXML(parts[1]);
                String colHeader = StringUtil.fixForXML(parts[2]);
                String key = String.format("\t<%s>%s</%s>", "X", colName, "X");
                String element = parts[0] + "_" + colHeader;
                if (element.startsWith("_")) element = element.substring(element.lastIndexOf("_")+1);
                String value = String.format("\t<%s>", element) +  numberFormat.format(sv.value) + String.format("</%s>\n", element);
                value += String.format("\t\t<%s_view>", element) + sv._view + String.format("</%s_view>", element);

                if (!columns.contains(colHeader)) columns.add(colHeader);
                if (!mapToAgg.containsKey(key)) mapToAgg.put(key, new ArrayList<String>());
                mapToAgg.get(key).add(value);
            } catch (Throwable t) {
                LOGGER.warn("Failed to Table-ise/handle:" + sv.label, t);
            }
        }
        List<String> allColumns = getAllColumns(mapToAgg);
        StringBuilder xml = new StringBuilder("<xml>");
        int rows = 0;
        for (String key : mapToAgg.keySet()) {
            if (rows++ >= topLimit) continue;
            xml.append("<item>\n");
            List<String> possibleValues = mapToAgg.get(key);
            Collections.sort(possibleValues);
            xml.append(key);
            List<String> thisRowsColumns = new ArrayList<String>(allColumns);
            for (String value : possibleValues) {
                xml.append(value);
                String removeColumn = getXMLElement(value);
                thisRowsColumns.remove(removeColumn);
            }
            for (String remaining : thisRowsColumns) {
                xml.append(String.format("<%s></%s>", remaining, remaining));
            }

            xml.append("</item>\n");

        }
        xml.append("</xml>");
        item.meta.allSeriesNames = columns;
        item.meta.xml = xml.toString();
    }
    transient DecimalFormat format;
    private String getUIFriendlyDecimal(ClientHistoItem.SeriesValue sv) {
        if (format == null) format = new DecimalFormat("#.####");
        return format.format(sv.value);
    }

    // need to extract out common values according to label_XXXXX - where XXXX is the group
    // Would be this....
    //			<xml>
    //			<item>	<msg>msg_host1</msg>		<msg_val>4</msg_val></item>
    //			<item>	<msg>pid_host2</msg>		<msg_val>1</msg_val></item>
    //			<item>	<msg>process_host2</msg>		<msg_val>1</msg_val></item>
    //			<item>	<msg>process_host1</msg>		<msg_val>3</msg_val></item>
    //			<item>	<msg>msg_host2</msg>		<msg_val>1</msg_val></item>
    //			<item>	<msg>pid_host1</msg>		<msg_val>3</msg_val></item>
    //			</xml>

    // We Want this
    //			<xml>
    //			<item>  <msg>host1</msg> <msg>4</msg> <pid>3</pid> <process>3</process> </item>
    //			<item>  <msg>host2</msg> <msg>2</msg> <pid>1</pid> <process>1</process> </item>
    //			</xml>

    public String convertToGroups(Map<String, ClientHistoItem.SeriesValue> series, String groupName2, String topFieldName, int topLimit, List<String> seriesList, boolean isTop) {
        final String useGroupName = groupName2.indexOf("-") > -1 ? groupName2.substring(0, groupName2.lastIndexOf("-")) : groupName2;
        StringBuilder xml = new StringBuilder("<xml>\n");
        NumberFormat numberFormat = NumberFormat.getInstance();
        List<String> groupNames = getXMLGroupNames(series, topFieldName, topLimit, seriesList, isTop);
        int added = 0;
        for (String groupName : groupNames) {
            if (added++ >= topLimit) continue;
            xml.append("<item>");
            List<ClientHistoItem.SeriesValue> seriesValuesForGroup = getXMLSeriesValuesForGroup(groupName, series, seriesList);
            xml.append(String.format("\t<%s>", useGroupName)).append(groupName).append(String.format("</%s>", useGroupName));
            for (ClientHistoItem.SeriesValue sv : seriesValuesForGroup) {
                String tag = sv.label.split(LogProperties.getFunctionSplit())[0];
                if (tag.equals(groupName2)) {
                    if (tag.contains("-")) tag = tag.substring(tag.indexOf("-")+1);
                    else tag = tag + "-";
                }
                xml.append(String.format("\t\t<%s>", tag)).append(numberFormat.format(sv.value)).append(String.format("</%s>", tag));
                xml.append(String.format("\t\t<%s_view>", tag)).append(sv._view).append(String.format("</%s_view>", tag));

            }
            xml.append("</item>\n");
        }
        xml.append("</xml>");

        return xml.toString();
    }
    private String getXMLElement(String value) {
        return value.substring(value.indexOf("<")+1, value.indexOf(">"));
    }
    private List<String> getAllColumns(Map<String, List<String>> mapToAgg) {
        List<String> allCols = new ArrayList<String>();
        Collection<List<String>> values = mapToAgg.values();
        for (List<String> list : values) {
            for (String value : list) {
                String element = getXMLElement(value);
                if (!allCols.contains(element)) allCols.add(element);
            }
        }
        return allCols;
    }
    public List<String> getXMLGroupNames(Map<String, ClientHistoItem.SeriesValue> series2, String topFieldName, int topLimit, List<String> seriesList, boolean isTop) {

        if (topFieldName != null && topFieldName.trim().length() > 0) {
            return getTopGroupsForFieldName(series2, topFieldName, topLimit, isTop);
        } else {
            return getSimpleTopGroups(seriesList, isTop);
        }
    }
    private List<String> getTopGroupsForFieldName(Map<String, ClientHistoItem.SeriesValue> series2, String topFieldName, int topLimit, boolean isTop) {

        List<ClientHistoItem.SeriesValue> seriesLabelsSortedByTop = new ArrayList<ClientHistoItem.SeriesValue>();
        for (ClientHistoItem.SeriesValue seriesValue : series2.values()) {
            if (seriesValue.label.contains(topFieldName)) seriesLabelsSortedByTop.add(seriesValue);
        }
        Collections.sort(seriesLabelsSortedByTop, new Comparator<ClientHistoItem.SeriesValue>() {
            public int compare(ClientHistoItem.SeriesValue o1, ClientHistoItem.SeriesValue o2) {
                // REVERSE this is we want bottom and not top!
                return Double.valueOf(o2.value).compareTo(o1.value);
            }
        });

//		if (seriesLabelsSortedByTop.size() > topLimit) seriesLabelsSortedByTop = seriesLabelsSortedByTop.subList(0, topLimit);
        List<String> seriesListSortedByTop = new ArrayList<String>();
        for (ClientHistoItem.SeriesValue seriesValue : seriesLabelsSortedByTop) {
            seriesListSortedByTop.add(seriesValue.label);
        }

        return getSimpleTopGroups(seriesListSortedByTop, isTop);
    }
    private List<String> getSimpleTopGroups(List<String> seriesList, boolean isTop) {
        List<String> groups = new ArrayList<String>();

        for (String label : seriesList) {
            if (label.contains(LogProperties.getFunctionSplit())) {
                String tag = label.substring(label.indexOf(LogProperties.getFunctionSplit())+1, label.length());

                if (!groups.contains(tag)) groups.add(tag);
            }
        }
        if (!isTop) Collections.reverse(groups);
        return groups;
    }
    public List<ClientHistoItem.SeriesValue> getXMLSeriesValuesForGroup(final String groupToFetch, Map<String, ClientHistoItem.SeriesValue> series2, final List<String> seriesList) {
        ArrayList<ClientHistoItem.SeriesValue> results = new ArrayList<ClientHistoItem.SeriesValue>(series2.values());
        Collections.sort(results, new Comparator<ClientHistoItem.SeriesValue>(){
            @Override
            public int compare(ClientHistoItem.SeriesValue o1, ClientHistoItem.SeriesValue o2) {
                int o1Pos = 0;
                int o2Pos = 0;
                for (int i = 0; i < seriesList.size(); i++){
                    String label = seriesList.get(i);
                    if (o1.label.equals(label)) o1Pos = i;
                    if (o2.label.equals(label)) o2Pos = i;
                }
                return Integer.valueOf(o1Pos).compareTo(o2Pos);
            }
        });
        CollectionUtils.filter(results, new Predicate() {

            public boolean evaluate(Object arg0) {
                ClientHistoItem.SeriesValue sv = (ClientHistoItem.SeriesValue) arg0;
//				return sv.label.contains(groupToFetch);
                return sv.label.endsWith(groupToFetch);
            }
        });
        return results;

    }




}
