package com.liquidlabs.log.space.agg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;

import com.liquidlabs.log.space.agg.ClientHistoItem.PieValue;
import com.liquidlabs.log.space.agg.ClientHistoItem.SeriesValue;
import org.json.JSONObject;
import org.json.XML;

public class Meta {
	// Stores series information used by the client to build the chart series (only held in the first clientHistoItem)
	public Map<String, SeriesValue> seriesSetup  = new HashMap<String, SeriesValue>();
	public List<String> allSeriesNames = new ArrayList<String>();
	
	// used for table representation
	public String xml;		
	public List<PieValue> pieChartData;
	
	// needed to support chartMax(100+) (revisit to support groupId and chartmax
	public Map<Integer,Double> maxValues = new HashMap<Integer,Double>();
	
	public Meta() {
	}

	public void toJSon(){
        if (xml != null && xml.length() > 0) {
            try {
            	if (xml.startsWith("{\"xml\":")) return;
//            	System.out.println("XML:" + xml);
                JSONObject jsonO = XML.toJSONObject(xml);
                jsonO.getJSONObject("xml").append("seriesList",allSeriesNames);
                xml = jsonO.toString();
				//System.out.println("JSON:" + xml);
			} catch (JSONException e) {
                System.out.println("JSON:" + xml);
				e.printStackTrace();
			}
      }
	}
}
