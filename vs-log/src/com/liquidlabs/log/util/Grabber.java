package com.liquidlabs.log.util;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 09/04/2013
 * Time: 08:35
 * To change this template use File | Settings | File Templates.
 */
public interface Grabber {

    static final String DD = "d";
    static final String MM = "MM";
    static final String YY = "yy";
    static final String MMM = "MMM";

    /**
     * True when we are using dd/MM/yyy etc - false where a scan is involved - i.e. json or char based
     * @return
     */
    boolean isRegularFormatter();

    Date parse(String string, long previousTimeExtracted);

    String format();

    String grab(String nextLine);

    boolean isFormatStringCutParsable();

    boolean isFormattingMatchCutSymbols(String cut);
}
