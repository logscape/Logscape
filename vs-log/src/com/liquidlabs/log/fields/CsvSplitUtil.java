package com.liquidlabs.log.fields;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by neil on 14/03/17.
 */
public class CsvSplitUtil implements Extractor {

    public CsvSplitUtil(){};

    public String[] extract(String toParse){
        if(toParse.isEmpty()) return new String[0];
        List<String> split = new ArrayList<>();
        boolean isCurrentlyEscaped = false;
        int lastSplit = 0;

        for(int i = 0; i < toParse.length(); i++){
            char curCar = toParse.charAt(i);
            if(curCar == '"') {
                isCurrentlyEscaped = !isCurrentlyEscaped;
                continue;
            }

            if(curCar == ',' && !isCurrentlyEscaped) {
                split.add(toParse.substring(lastSplit, i));
                lastSplit = i+1;
            } else if( i == toParse.length() -1){
                split.add(toParse.substring(lastSplit));
            }
        }

        return split.toArray(new String[0]);
    }

    @Override
    public void reset() {

    }
}
