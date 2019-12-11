package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.LogProperties;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.*;

public class ClientHistoItem {
	private final static Logger LOGGER = Logger.getLogger(ClientHistoItem.class);
	
	public String time = "";
	public long timeMs = 0;
	
	// Stores series data
	public Map<String, SeriesValue> series = new TreeMap<String, SeriesValue>();
	public Meta meta;
	
	
	public static class SeriesValue implements Serializable {
		private static final long serialVersionUID = 1L;
		
		public int sPos;
		public int queryPos;
		public int groupId;
		public String label;
		public double value;
		public String fieldName = "";
		public String func = "";
		public String _view = "";

		public SeriesValue() {
		}
		public SeriesValue(String label) {
			setLabelValue(label);
		}
		private void setLabelValue(String label2) {
			this.label = label2;
		}
		public void setViewValue(String view){
			this._view = view;
		}

		public SeriesValue(String label, double value, int queryPos, int groupId) {
			set(label, value, queryPos, groupId, 0);
		}
		public void set(String label, double value, int queryPos, int groupId) {
			set(label, value, queryPos, groupId, 0);
		}
		public void set(String label, double value, int queryPos, int groupId, int sourcePos) {
			setLabelValue(label);
			this.value = value;
			this.queryPos = queryPos;
			this.groupId = groupId;
			this.sPos = sourcePos;
		}
		
		public double getValue() {
			return value;
		}
		public void increment(double value){
			this.value += value;
		}

		public void setValue(double value) {
			this.value = value;
		}
		public String toString() {
			return "SeriesValue:"+label +" v:" + value + " p:" + queryPos + " l:" + label + " grp:" + this.groupId + "\n";
		}
		public int hashCode() {
			return this.label.hashCode();
		}
		public boolean equals(Object obj) {
			return this.label.equals(((SeriesValue)obj).label);
		}
		public String toXMLReportString(String time) {
//			<row series="s0" date="2009-03-20"  v1="631" />
			return String.format("<row series=\"%s\" date=\"%s\" v1=\"%f\"/>\n", StringEscapeUtils.escapeXml(label), time, value);
		}

		public void escapeForPresentation() {
			this.label = this.label.replaceAll(LogProperties.getFunctionSplit(), "_");
			
		}
	}
		
	public SeriesValue get(String seriesName){
		return series.get(seriesName);
	}
	public Collection<String> series(){
		return series.keySet();
	}

	public ClientHistoItem(String label, double value, int queryPos, int groupId, int sourcePos) {
		set(label, value, queryPos, groupId, sourcePos);
	}

	public ClientHistoItem() {
	}

	public void setField(String label, String fieldName, String function, String view) {
		SeriesValue sv = series.get(label);
		if (sv == null) {
			sv = new SeriesValue();
			series.put(label, sv);
		}
		sv.fieldName = fieldName;
		sv.func = function;
        sv._view = view;
	}

	
	public interface HistoSetter {
		void setIt(SeriesValue val);
	}
	
	public void setTimeValue(long timeMs) {
		if (isToday(timeMs)) {
			this.time = DateUtil.shortTimeFormatter3.print(timeMs);
		} else {
			this.time = DateUtil.shortDateTimeFormat3.print(timeMs);
		}
		this.timeMs = timeMs;
	}
	private boolean isToday(long timeMs) {
		DateTime now = new DateTime();
		DateTime given = new DateTime(timeMs);
		return now.getDayOfYear() == given.getDayOfYear();
	}

	public void increment(String label, int incrementAmount, int queryPos, int groupId) {
		if (!series.containsKey(label)) {
			SeriesValue sv = new SeriesValue();
			sv.set(label, 0, queryPos, groupId);
			series.put(label, sv);
		}
		SeriesValue seriesValue = series.get(label);
		seriesValue.value = seriesValue.value + incrementAmount;
		seriesValue.queryPos = queryPos;
		seriesValue.groupId = groupId;
		
	}
	public SeriesValue incrementSpecial(String label, int incrementAmount, int queryPos, int groupId) {
		if (meta == null) {
			meta = new Meta();
			
		}
		if (!meta.seriesSetup.containsKey(label)) {
			SeriesValue sv = new SeriesValue();
			sv.set(label, 0, queryPos, groupId);
			meta.seriesSetup.put(label, sv);
		}
		SeriesValue seriesValue = meta.seriesSetup.get(label);
		seriesValue.value = seriesValue.value + incrementAmount;
		seriesValue.queryPos = queryPos;
		seriesValue.groupId = groupId;
		return seriesValue;
	}
	
	public String toString(){
        if (this.series.size() < 5) {
            return "Histo:" + time + " count:" + this.series + "\n";
        }
		return "Histo:" + time + " count:" + this.series.size() + "\n";
	}


	public int getTotalCount() {
		int result = 0;
		for (SeriesValue sv : getSeriesSortedByQueryPos()) {
			result += sv.value;
		}
		return result;
	}
	public int getTotalHits() {
		int result = 0;
		for (SeriesValue sv : getSeriesSortedByQueryPos()) {
			if (sv.value > 0) result++;
		}
		return result;
	}

	/**
	 * Preserve function ordering
	 * @param queryPos
	 * @return
	 */
	public List<SeriesValue> getSeriesNames(int queryPos) {
		Set<String> added = new HashSet<String>();
		List<SeriesValue> svs = new ArrayList<SeriesValue>();
		
		for (SeriesValue sv : this.series.values()) {
			if (added.contains(sv.label)) continue;
			if (sv.queryPos == queryPos) {
				svs.add(sv);
				added.add(sv.label);
			}
		}
//		Collections.sort(svs, new Comparator<SeriesValue>(){
//			public int compare(SeriesValue o1, SeriesValue o2) {
//				return Integer.valueOf(o1.groupId).compareTo(o2.groupId);
//			}
//		});
//		List<String> result = new ArrayList<String>();
//		for (SeriesValue seriesValue : svs) {
//			result.add(seriesValue.label);
//		}
		return svs;
	}
	public int getGroupIdForSeries(String seriesName, int pos) {
		for (SeriesValue sv : this.series.values()) {
			if (sv.queryPos == pos && sv.label.equals(seriesName)) return sv.groupId;
		}
		return -1;
	}

	private Collection<SeriesValue> getSeriesSortedByQueryPos() {
		
		HashMap<Integer, List<SeriesValue>> stuff = new HashMap<Integer, List<SeriesValue>>();
		for (SeriesValue sv : this.series.values()) {
			if (!stuff.containsKey(sv.queryPos)) {
				stuff.put(sv.queryPos, new ArrayList<SeriesValue>());
			}
			stuff.get(sv.queryPos).add(sv);
		}
		for (List<SeriesValue> svList : stuff.values()) {
			Collections.sort(svList, new Comparator<SeriesValue>(){
				public int compare(SeriesValue o1, SeriesValue o2) {
					return o1.label.compareTo(o2.label);
				}
			});
		}
		ArrayList<Integer> sortedKeys = new ArrayList<Integer>(stuff.keySet());
		Collections.sort(sortedKeys);
		
		ArrayList<SeriesValue> results = new ArrayList<SeriesValue>();
		for (Integer sortedKey : sortedKeys) {
			results.addAll(stuff.get(sortedKey));
		}
		return results;
	}

	public int getSeriesCount() {
		return series.size();
	}
	public int getGroupCount() {
		Collection<SeriesValue> values = this.series.values();
		Set<Integer> groupIds = new HashSet<Integer>();
		for (SeriesValue seriesValue : values) {
			groupIds.add(seriesValue.groupId);
		}
		return groupIds.size();
	}


	public SeriesValue getSeriesValue(String label) {
		return this.series.get(label);
	}

	public SeriesValue getSeries(String string) {
		return this.series.get(string);
	}

	public long getStartTime() {
		return this.timeMs;
	}
	public boolean evict(String evicteeName, boolean topOther) {
		if (evicteeName != null) {
			SeriesValue remove = series.remove(evicteeName);
			if (remove != null && topOther) {
				String otherName = "Other_" + (remove.queryPos+1);
				SeriesValue seriesValue = series.get(otherName);
				if (seriesValue == null) {
					seriesValue = new SeriesValue(otherName,0,remove.queryPos,remove.groupId);
					series.put(otherName, seriesValue);
				}
				seriesValue.setValue(seriesValue.getValue() + remove.getValue());
				
			}
			return remove != null;
		}
		return false;
	}
	public HashSet<String> evict(List<String> evicting, boolean topOther) {
		HashSet<String> result = new HashSet<String>();
		for (String string : evicting) {
			if (string.contains(".by.")) continue;
			if (evict(string, topOther)) result.add(string);
		}
		return result;
	}
	public void replacePos(int position, int sourcePosition) {
		if (position == sourcePosition) return;
		for (SeriesValue sv : this.series.values()) {
			if (sv.queryPos == position) sv.queryPos = sourcePosition;
		}
	}
	public Collection<String> keys() {
		return series.keySet();
	}
	public void applyPieChartData(List<ClientHistoItem> result) {
		Set<String> seriesName = this.series.keySet();
		if (meta == null) meta = new Meta();
		this.meta.pieChartData = new ArrayList<PieValue>();
		for (String series : seriesName) {
			SeriesValue seriesValue = this.series.get(series);
			this.meta.pieChartData.add(new PieValue(series, seriesValue.getValue()));
		}
		Collections.sort(this.meta.pieChartData, new Comparator<PieValue>(){
			public int compare(PieValue o1, PieValue o2) {
				// should be this direction to allow for pie labelling to work better (it renders the first n items)
				return Double.valueOf(o1.value).compareTo(o2.value);
			}
		});
	}
	public static class PieValue {
		public PieValue() {
		}
		public PieValue(String label, Double value) {
			this.label = label.replaceAll("!", "_").replaceAll(" ", "_");
			this.value = value;
		}
		public String label;
		public Double value;
	}

	String getAutoGroupName(SeriesValue seriesValue) {
		return seriesValue.label.substring(0, seriesValue.label.indexOf(LogProperties.getFunctionSplit()));
	}
	
	String getGroupableName(List<String> seriesList) {
		String firstItem = seriesList.get(0);
		return firstItem.substring(0,firstItem.indexOf(LogProperties.getFunctionSplit()));
	}


	//			<xml>
	//			<item> <-Time>12:30</-Time>  <msg>host1</msg> <msg>4</msg> <pid>3</pid> <process>3</process> </item>
	//			<item>  <-Time>12:30</-Time> <msg>host2</msg> <msg>2</msg> <pid>1</pid> <process>1</process> </item>
	//			</xml>
	public String adaptTimeSeriesToXML(List<ClientHistoItem> result) {
		NumberFormat numberFormat = NumberFormat.getInstance();
		// display newest at the top of the list
		List<String> allSeriesNames = result.get(0).meta.allSeriesNames;
		
		
		List<ClientHistoItem> copy =new ArrayList<ClientHistoItem>(result); 
		Collections.reverse(copy);
		DateTimeFormatter formatter = DateUtil.shortDateTimeFormat2;
		StringBuilder xml = new StringBuilder("<xml>\n");
		for (ClientHistoItem clientHistoItem : copy) {
			xml.append("<item>");
			xml.append("<Time>").append(formatter.print(clientHistoItem.timeMs)).append("</Time>");
			for (String string : allSeriesNames) {
				SeriesValue sv = clientHistoItem.series.get(string);
				if (sv == null) sv = new SeriesValue(string);
				String label = StringUtil.fixForXML(sv.label);
				double value = sv.value;
				xml.append(String.format("<%s>%s</%s>", label, numberFormat.format(value), label));
			}
			xml.append("</item>\n");
		}
		
		xml.append("</xml>");
		this.meta.xml = xml.toString();
		return xml.toString();
		
	}

	public void escapeForPresentation() {
		if (this.series != null) {
			Set<String> keySet = new HashSet<String>(this.series.keySet());
			for (String seriesKey : keySet) {
				SeriesValue sv = this.series.remove(seriesKey);
				sv.escapeForPresentation();
				this.series.put(sv.label, sv);
			}
		}
		
		if (this.meta != null && this.meta.allSeriesNames != null) {
			List<String> newSeriesNames = new ArrayList<String>();
			for (String seriesNames : this.meta.allSeriesNames) {
				newSeriesNames.add(seriesNames.replaceAll(LogProperties.getFunctionSplit(), "_").replaceAll("'", ""));
			}
			this.meta.allSeriesNames = newSeriesNames;
		}
		if (this.meta != null && this.meta.seriesSetup != null) {
			for (String seriesKey : new ArrayList<String>(this.meta.seriesSetup.keySet())) {
				SeriesValue remove = this.meta.seriesSetup.remove(seriesKey);
				remove.escapeForPresentation();
				this.meta.seriesSetup.put(remove.label, remove);
			}
		}
	}

	public void set(String group, Object val, int queryPos, int groupId, int querySourcePos) {
		if (val instanceof Integer) {
			set(group, ((Integer) val).doubleValue(), queryPos, groupId, querySourcePos);
		} else if (val instanceof Double) {
			set(group, ((Double) val).doubleValue(), queryPos, groupId, querySourcePos);
		} else if (val instanceof Float) {
			set(group, ((Float) val).doubleValue(), queryPos, groupId, querySourcePos);
		} else if (val instanceof Long) {
            set(group, ((Long) val).doubleValue(), queryPos, groupId, querySourcePos);
        }
	}
	public void set(String label, double value, int queryPos, int groupId, int sourcePos){
		if (value >= 100) {
			value = new Double(value).intValue();
		} else if (value > 2) {
			value = StringUtil.roundDouble(value, 2);
		}
		SeriesValue sv = series.get(label);
		if (sv == null) {
			sv = new SeriesValue();
			series.put(label, sv);
		}
		sv.set(label, value, queryPos, groupId, sourcePos);
	}

	/**
	 * Try and merge in any items with this time scope
	 * @param foundHisto
	 * @param fromMs 
	 * @param toMs 
	 * @param firstItem
	 */
	public void merge(List<ClientHistoItem> foundHisto, long fromMs, long toMs, ClientHistoItem firstItem) {
		int hits = 0;
		for (ClientHistoItem otherItem : foundHisto) {
			if (otherItem.timeMs >= fromMs && otherItem.timeMs < toMs){
				this.series = otherItem.series;
				hits++;
				ClientHistoItem otherFirst = foundHisto.get(0);
				firstItem.meta = otherFirst.meta;
				
			}
		}
		if (hits > 0) return;
		increment("*", 0, 0, 0);
		incrementSpecial("*", 0, 0, 0);
		if (!firstItem.meta.allSeriesNames.contains("*")) firstItem.meta.allSeriesNames.add("*");
	}
	public void getMaxValue(Map<Integer, Double> maxValueMap) {
		for (SeriesValue value : series.values()) {
			int groupId = value.groupId;
			if (!maxValueMap.containsKey(groupId) || maxValueMap.get(groupId) < value.value) maxValueMap.put(groupId, value.value);
		}
	}
}
