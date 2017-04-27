package com.liquidlabs.log.fields;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 08:47
 * To change this template use File | Settings | File Templates.
 */
public interface Extractor {
    String[] extract(final String nextLine);
    void reset();
}
