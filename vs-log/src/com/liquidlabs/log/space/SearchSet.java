package com.liquidlabs.log.space;

import com.liquidlabs.orm.Id;

import java.util.ArrayList;
import java.util.List;

/**
 * This object is also called a Dashboard on the GUI
 *
 */
public class SearchSet {
	@Id
	public String name;
	
	public String owner;
	public int durationHours = 2;
	public int columns = 2;
	
	enum SCHEMA { name, searches, owner, durationHours,columns };
	
	public List<String> searches = new ArrayList<String>();
	
	public SearchSet() {
	}
	public SearchSet(String name, List<String> searches, String owner, int durationHours, int columns) {
		this.name = name;
		this.searches = new ArrayList<String>(searches);
		this.owner = owner;
		this.durationHours = durationHours;
		this.columns = columns;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<String> getSearches() {
		return searches;
	}
	public void setSearches(List<String> searches) {
		this.searches = searches;
	}
	
	public String toString() {
		return super.toString() + " :" + name + " s:" + searches;
	}
	public void addSearch(String searchName) {
		if (!this.searches.contains(searchName)) searches.add(searchName);
	}
    static String controllerJson = "{\"col\":%d,\"row\":1,\"size_x\":2,\"size_y\":1,\"id\":\"widget-1361552899714\",\"type\":\"controller_widget\",\"configuration\":{\"timeMode\":\"Standard\",\"period\":\"3600\",\"fromTime\":\"2013-02-22T15:36:54.483Z\",\"toTime\":\"2013-02-22T16:36:54.483Z\"}}";
    static String searchJson = "{\"col\":%d,\"row\":%d,\"size_x\":%d,\"size_y\":%d,\"id\":\"widget-%d\",\"type\":\"chart_widget\",\"configuration\":{\"title\":\"%s\",\"widgetId\":\"#chart_widget-%d\",\"terms\":[\"%s\"]}}";
    public String toJSON(LogSpace logspace) {
        // add a controller to pos 1,1

        int chartWidth = 2;
        int col = 1;
        int row = 1;
        int w = chartWidth * 2;
        int y = 1;
        int lastRow = 1;
        boolean inGroup = false;
        StringBuilder sb = new StringBuilder("["+String.format(controllerJson, chartWidth * 2 +1));
        for (int i = 0; i < searches.size(); i++) {

            String name = searches.get(i);
            if (name.equals("[")) {
                inGroup = true;
                w = chartWidth;
                continue;
            } else if (name.equals("]")) {
                inGroup = false;
                w = chartWidth * 2;
                row += y;
                continue;
            }
            //if (i > 0)
                sb.append(",\n");

            Search search = logspace.getSearch(name, null);

            if (search != null) {
                String searchTerm = search.patternFilter.get(0);
                String jsonString = String.format(searchJson,col,row,w,y,System.currentTimeMillis(),name, System.currentTimeMillis(), searchTerm);
                sb.append(jsonString).append("\n");
                if (inGroup) {
                    if (col == 1) col = w+1;
                    else col = 1;
                } else {
                    row += y;
                }
            }
        }
        return sb.append("]").toString();
    }
}

