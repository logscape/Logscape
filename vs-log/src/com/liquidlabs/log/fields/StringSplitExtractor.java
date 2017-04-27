package com.liquidlabs.log.fields;

import com.liquidlabs.common.regex.StringSplitEval;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 09:53
 * To change this template use File | Settings | File Templates.
 */
public class StringSplitExtractor implements Extractor {

    private final StringSplitEval stringSplitEval;

    public StringSplitExtractor(String expression) {
        stringSplitEval = new StringSplitEval(expression);
    }

    @Override
    public String[] extract(String nextLine) {
        String[] values = stringSplitEval.evaluateGroups(nextLine);
        for (int i = 0; i < values.length; i++) {
            if (values[i].startsWith("\"")) values[i] = values[i].replace("\"","");
        }
        return values;
    }

    @Override
    public void reset() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
