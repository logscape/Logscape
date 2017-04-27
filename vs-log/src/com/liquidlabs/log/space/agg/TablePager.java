package com.liquidlabs.log.space.agg;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.util.DateTimeExtractor;
import com.liquidlabs.transport.serialization.Convertor;

public class TablePager implements KryoSerializable {
	

	private static final int VIEW_INDEX = 3;
	private static final int VALUE_INDEX = 2;
	private String subscriber;
	//rowColValueView ordering
	private LinkedHashMap<String, List<String[]>> rowData;
	private int pageSize;
	private String groupByFieldname;
	private String groupByView;
	private String tag;
	// top or bottom results

	public TablePager(String subscriber, String groupByFieldname, int pageSize, String tag, LinkedHashMap<String, List<String[]>> rowData, boolean isTop) {
		this.subscriber = subscriber;
		this.groupByFieldname = StringUtil.fixForXML(groupByFieldname);
//		this.groupByView = groupByView.replaceAll("<", "&lt;").replaceAll(">", "&gt;");
		this.pageSize = pageSize;
		this.tag = tag;
		this.rowData = rowData;
	}
	
	// write me off to disk
	public static void write(ScheduledExecutorService scheduler, final TablePager pager) {
		Runnable writeTask = new Runnable() {
			public void run() {
				TablePager.writeData(pager);
			}
		};
		scheduler.schedule(writeTask, 1, TimeUnit.SECONDS);
		
		final File toDelete = pager.getFilename();
		toDelete.deleteOnExit();

		Runnable deleteTask = new Runnable() {
			public void run() {
				toDelete.delete();
			}
		};
		scheduler.schedule(deleteTask, 2, TimeUnit.HOURS);
	}

	private static void writeData(TablePager pager) {
		try {
			byte[] serializeAndCompress = Convertor.serializeAndCompress(pager);
			File file = pager.getFilename();
			file.deleteOnExit();
			file.getParentFile().mkdirs();
			BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(file));
			os.write(serializeAndCompress);
			os.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private File getFilename() {
		return new File("work/tables/" + subscriber + ".ser");
	}
	public TablePager() {
	}
	public static TablePager read(String subscriber) throws IOException, ClassNotFoundException {
		File file = new File("work/tables/" + subscriber + ".ser");
		if (!file.exists()) throw new RuntimeException("File Not Found!:" + subscriber);
		FileInputStream fis = new FileInputStream(file);
		byte[] data = new byte[fis.available()];
		fis.read(data);
		fis.close();
		return (TablePager) Convertor.deserializeAndDecompress(data);
	}


    List<String> tableColumnnNames = null;
	public String getPage(int page, int sortedColumn, boolean ascendingOrder) {
		
		StringBuilder xml = new StringBuilder("<xml>\n");

		int items = 0;
		
		// sort it
		if (sortedColumn != -1) sortData(sortedColumn-1, ascendingOrder);
		
		int totalPages = (rowData.size() / pageSize) + 1;
		
		boolean writtenPageCount = false;
		Set<String> allColumnNames = getAllColumnNames(page);
		int total = rowData.keySet().size();
        tableColumnnNames = new ArrayList<String>();
		
		for (String key : rowData.keySet()) {
			List<String[]> row = rowData.get(key);
			int thisPage = items++ / pageSize;
			
			// find the page
			if (thisPage +1  != page) {
				continue;
			}
			xml.append("<item>");
			
			String rowName = row.get(0)[0];
			xml.append(String.format("<%s>%s</%s>", groupByFieldname, StringUtil.fixForXML(rowName), groupByFieldname));
            if (!tableColumnnNames.contains(groupByFieldname)) tableColumnnNames.add(groupByFieldname);

			
			
			Set<String> handledCols = new HashSet<String>();
			for (String[] rowColValueView : row) {
				String value = rowColValueView[VALUE_INDEX];
				if (value == null) continue;
				//rowColValueView = StringUtil.fixForXML(rowColValueView);
				// keep the value though
				value = rowColValueView[VALUE_INDEX];
				String column = getColumnName(rowColValueView);
				handledCols.add(column);
                column = column.replace("/","_");
                if (!tableColumnnNames.contains(column)) tableColumnnNames.add(column);
				xml.append(String.format("\t\t<%s>", column)).append(formatValue(value)).append(String.format("</%s>", column));
				xml.append(String.format("\t\t<%s_view>", column)).append(rowColValueView[VIEW_INDEX]).append(String.format("</%s_view>", column));
				
				if (!writtenPageCount) {
					xml.append("<_pages_>" + totalPages + "</_pages_>");
					writtenPageCount = true;
				}
			}
			// now include missing columns
			if (handledCols.size() != allColumnNames.size()) {
				Set<String> missingCols = new HashSet<String>(allColumnNames);
				missingCols.removeAll(handledCols);
				for (String column : missingCols) {
					column = StringUtil.fixForXML(column);
					xml.append(String.format("\t\t<%s>", column)).append("").append(String.format("</%s>", column));
					xml.append(String.format("\t\t<%s_view>", column)).append("").append(String.format("</%s_view>", column));
				}
			}
			xml.append("</item>\n");
		}
        xml.append("</xml>");
		return xml.toString();
	}

	transient DecimalFormat formatter4 = new DecimalFormat("###,###,###.####");
	transient DecimalFormat formatter2 = new DecimalFormat("###,###,###.##");
	transient DecimalFormat formatter0 = new DecimalFormat("###,###,###");
	private String formatValue(String value) {


		Double double1 = StringUtil.isDouble(value);
		if (double1 == null) return value;
		if (double1 > 100) return formatter0.format(double1);
		if (double1 > 2) return formatter2.format(double1);
		return formatter4.format(double1);
	}

	private Set<String> getAllColumnNames(int page) {
		Set<String> results = new HashSet<String>();
		int items = 0;
		for (String key : rowData.keySet()) {
			List<String[]> row = rowData.get(key);
			int thisPage = items++ / pageSize;
			
			// find the page
			if (thisPage +1  != page) {
				continue;
			}
			for (String[] rowColValueView : row) {
				String columnName = StringUtil.fixForXML(getColumnName(rowColValueView));
				results.add(columnName);
			}
			
		}
		return results;
	}

	public String getColumnName(String[] rowColValueView) {
		String fieldLabel = rowColValueView[1];
		String function = rowColValueView[5];
		String extension = "-val";
		if (function != null && function.length() > 0) extension = "-" + function;
		String column = "";
		// fieldLabels = WARN- - dont display extension
		if (fieldLabel.contains("-")) {
			if (fieldLabel.endsWith("-")) fieldLabel = fieldLabel.substring(0, fieldLabel.length()-1);
			extension = "";
			column = fieldLabel + extension;
		}
		column = fieldLabel + extension;
		return column;
	}

	enum SortType { key, string, integer, doub, date }
	private void sortData(final int gSortedColumn, final boolean ascendingOrder) {
			try {
			
			final SortType sortType = gSortedColumn >= 0 ? getSortType(rowData.values().iterator().next().get(gSortedColumn)[VALUE_INDEX]) : getSortType(rowData.values().iterator().next().get(0)[0]);
			final long now = System.currentTimeMillis();
			final DateTimeExtractor extractor = new DateTimeExtractor();
			
			ArrayList<String> allKeys = new ArrayList<String>(rowData.keySet());
			Collections.sort(allKeys, new Comparator<String>(){
				public int compare(String key1, String key2) {
					try {
						int sortedColumn = gSortedColumn >= 0 ? gSortedColumn : 0;
						List<String[]> rowOne = rowData.get(key1);
						List<String[]> rowTwo = rowData.get(key2);
						// column name
						if (sortType.equals(SortType.key)) {
							String column1 = rowOne.get(0)[0];
							String column2 = rowTwo.get(0)[0];
							if (ascendingOrder) return column1.compareTo(column2);
							else return column2.compareTo(column1);
						}
						String[] values1 = rowOne.get(sortedColumn);
						String v1 = values1[2];
						if (gSortedColumn == -1) v1 = values1[0];
						String[] values2 = rowTwo.get(sortedColumn);
						String v2 = values2[2];
						if (gSortedColumn == -1) v2 = values2[0];
						
						if (sortType == SortType.doub) {
							if (ascendingOrder) return new Double(Double.parseDouble(v1)).compareTo(Double.parseDouble(v2));
							else return new Double(Double.parseDouble(v2)).compareTo(Double.parseDouble(v1));
						}
						if (sortType == SortType.date) {
							Date time1 = extractor.getTime(v1, now);
							Date time2 = extractor.getTime(v2, now);
							if (ascendingOrder) return time1.compareTo(time2);
							else return time2.compareTo(time1);
						}
						
						if (ascendingOrder) return v1.compareTo(v2);
						return v2.compareTo(v1);
					} catch (Throwable t) {
						t.printStackTrace();
						return key2.compareTo(key1);
					}
				}
			});
			// now make a new data set
			int pos = 0;
			 LinkedHashMap<String, List<String[]>> newRowData = new  LinkedHashMap<String, List<String[]>>();
			 for (String key : allKeys) {
				List<String[]> list = rowData.get(key);
				if (list != null) {
					newRowData.put(key, list);
				}
				pos++;
			 }
			 
			 this.rowData = newRowData;
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	private SortType getSortType(String string) {
		// double sort for int or double
		if (StringUtil.isIntegerFast(string) || StringUtil.isDouble(string) != null) return SortType.doub;
		if (new DateTimeExtractor().getFormat(string, 0) != null) return SortType.date;
		return SortType.string;
	}

	public int getColumnIndex(String columnName) {
		List<String[]> columnInfo = rowData.values().iterator().next();
		for (int i = 0; i < columnInfo.size(); i++) {
			String[] strings = columnInfo.get(i);
			if (strings[1].equals(columnName)) return i+1;
		}
		return 0;
	}
	public void read(Kryo kryo, Input input) {
		this.subscriber = kryo.readObject(input, String.class);
		this.pageSize = kryo.readObject(input, int.class);
		this.groupByFieldname = kryo.readObject(input, String.class);
		this.groupByView = kryo.readObject(input, String.class);
		this.tag = kryo.readObject(input, String.class);
		rowData = (LinkedHashMap<String, List<String[]>>) kryo.readClassAndObject(input);
	}
	public void write(Kryo kryo, Output output) {
		
		kryo.writeObject(output, this.subscriber);
		kryo.writeObject(output, pageSize);
		kryo.writeObject(output, groupByFieldname);
		kryo.writeObject(output, groupByView);
		kryo.writeObject(output, tag);
		kryo.writeClassAndObject(output, rowData);		
	}
}
